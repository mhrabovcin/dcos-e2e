"""
Helpers for interacting with DC/OS Docker.
"""

import subprocess
import uuid
from ipaddress import IPv4Address
from pathlib import Path
from shutil import copyfile, copytree, ignore_patterns, rmtree
from typing import Any, Dict, Optional, Set

import docker
import yaml
from retry import retry

from ._common import Node, run_subprocess


class _ConflictingContainerError(Exception):
    """
    Raised when an existing container conflicts with a container which will be
    created.
    """

class DCOS_Docker:  # pylint: disable=invalid-name
    """
    A record of a DC/OS Docker cluster.
    """

    def __init__(
        self,
        masters: int,
        agents: int,
        public_agents: int,
        extra_config: Dict[str, Any],
        generate_config_path: Path,
        dcos_docker_path: Path,
        custom_ca_key: Optional[Path],
        log_output_live: bool,
        files_to_copy_to_installer: Dict[Path, Path],
        tmp_dir_path: Path=Path('/tmp'),
    ) -> None:
        """
        Create a DC/OS Docker cluster.

        Args:
            masters: The number of master nodes to create.
            agents: The number of agent nodes to create.
            public_agents: The number of public agent nodes to create.
            extra_config: DC/OS Docker comes with a "base" configuration.
                This dictionary can contain extra installation configuration
                variables.
            generate_config_path: The path to a build artifact to install.
            dcos_docker_path: The path to a clone of DC/OS Docker.
            custom_ca_key: A CA key to use as the cluster's root CA key.
            log_output_live: If `True`, log output of subprocesses live.
                If `True`, stderr is merged into stdout in the return value.
            files_to_copy_to_installer: A mapping of host paths to paths on
                the installer node. These are files to copy from the host to
                the installer node before installing DC/OS. Currently on DC/OS
                Docker the only supported paths on the installer are in the
                `/genconf` directory.
            tmp_dir_path: Path to temporary directory where we can create
                and copy necessary files.
        """
        self.log_output_live = log_output_live

        # To avoid conflicts, we use random container names.
        # We use the same random string for each container in a cluster so
        # that they can be associated easily.
        random = uuid.uuid4()

        # We create a new instance of DC/OS Docker and we work in this
        # directory.
        # This reduces the chance of conflicts.
        self._path = tmp_dir_path / 'dcos-docker-{random}'.format(random=random)

        copytree(
            src=str(dcos_docker_path),
            dst=str(self._path),
            # If there is already a config, we do not copy it as it will be
            # overwritten and therefore copying it is wasteful.
            ignore=ignore_patterns('dcos_generate_config.sh'),
        )

        copyfile(
            src=str(generate_config_path),
            dst=str(self._path / 'dcos_generate_config.sh'),
        )

        # Files in the DC/OS Docker directory's genconf directory are mounted
        # to the installer at `/genconf`.
        # Therefore, every file which we want to copy to `/genconf` on the
        # installer is put into the genconf directory in DC/OS Docker.
        for host_path, installer_path in files_to_copy_to_installer.items():
            relative_installer_path = installer_path.relative_to('/genconf')
            destination_path = self._path / 'genconf' / relative_installer_path
            copyfile(src=str(host_path), dst=str(destination_path))

        master_ctr = 'dcos-master-{random}-'.format(random=random)
        agent_ctr = 'dcos-agent-{random}-'.format(random=random)
        public_agent_ctr = 'dcos-public-agent-{random}-'.format(random=random)
        # Only overlay and aufs storage drivers are supported.
        # This chooses the aufs driver so the host's driver is not used.
        #
        # This means that the tests will run even if the storage driver on
        # the host is not one of these two.
        #
        # aufs was chosen as it is supported on the version of Docker on
        # Travis CI.
        client = docker.from_env(version='auto')
        host_storage_driver = client.info()['Driver']
        supported_storage_drivers = ('overlay', 'aufs')
        if host_storage_driver in supported_storage_drivers:
            docker_storage_driver = host_storage_driver
        else:
            docker_storage_driver = 'aufs'
        self._variables = {
            'DOCKER_STORAGEDRIVER': docker_storage_driver,
            # Some platforms support systemd and some do not.
            # Disabling support makes all platforms consistent in this aspect.
            'MESOS_SYSTEMD_ENABLE_SUPPORT': 'false',
            # Number of nodes.
            'MASTERS': str(masters),
            'AGENTS': str(agents),
            'PUBLIC_AGENTS': str(public_agents),
            # Container names.
            'MASTER_CTR': master_ctr,
            'AGENT_CTR': agent_ctr,
            'PUBLIC_AGENT_CTR': public_agent_ctr,
        }  # type: Dict[str, str]

        if extra_config:
            self._variables['EXTRA_GENCONF_CONFIG'] = yaml.dump(
                data=extra_config,
                default_flow_style=False,
            )

        if custom_ca_key is not None:
            master_mount = '-v {custom_ca_key}:{path}'.format(
                custom_ca_key=custom_ca_key,
                path=Path('/var/lib/dcos/pki/tls/CA/private/custom_ca.key'),
            )
            self._variables['MASTER_MOUNTS'] = master_mount

        self._create_containers()

    @retry(exceptions=_ConflictingContainerError, delay=10, tries=30)
    def _create_containers(self) -> None:
        """
        Create containers for the cluster.

        Creating clusters involves creating temporary installer containers.
        These containers can conflict in name.
        If a conflict occurs, retry.
        """
        # The error substring differs on different versions of Docker.
        conflict_error_substring = 'Conflict. The container name'
        other_conflict_error_substring = 'Conflict. The name'

        try:
            self._make(target='all')
        except subprocess.CalledProcessError as exc:
            # Handle error in stderr or stdout.
            # This is because if we log output live, stderr is redirected to
            # stdout.
            stderr = str(exc.stderr) + str(exc.stdout)
            conflict = conflict_error_substring in stderr
            conflict = conflict or other_conflict_error_substring in stderr
            if conflict:
                print(exc.stderr)
                raise _ConflictingContainerError()
            raise

    def _make(self, target: str) -> None:
        """
        Run `make` in the DC/OS Docker directory using variables associated
        with this instance.

        Args:
            target: `make` target to run.

        Raises:
            CalledProcessError: The process exited with a non-zero code.
        """
        args = ['make'] + [
            '{key}={value}'.format(key=key, value=value)
            for key, value in self._variables.items()
        ] + [target]

        run_subprocess(
            args=args,
            cwd=str(self._path),
            log_output_live=self.log_output_live
        )

    def postflight(self) -> None:
        """
        Wait for nodes to be ready to run tests against.
        """
        self._make(target='postflight')

    def destroy(self) -> None:
        """
        Destroy all nodes in the cluster.
        """
        self._make(target='clean')
        rmtree(
            path=str(self._path),
            # Some files may be created in the container that we cannot clean
            # up.
            ignore_errors=True,
        )

    def _nodes(self, container_base_name: str, num_nodes: int) -> Set[Node]:
        """
        Args:
            container_base_name: The start of the container names.
            num_nodes: The number of nodes.

        Returns: ``Node``s corresponding to containers with names starting
            with ``container_base_name``.
        """
        client = docker.from_env()
        nodes = set([])  # type: Set[Node]

        while len(nodes) < num_nodes:
            container_name = '{container_base_name}{number}'.format(
                container_base_name=container_base_name,
                number=len(nodes) + 1,
            )
            container = client.containers.get(container_name)
            ip_address = container.attrs['NetworkSettings']['IPAddress']
            node = Node(
                ip_address=IPv4Address(ip_address),
                ssh_key_path=self._path / 'include' / 'ssh' / 'id_rsa',
            )
            nodes.add(node)

        return nodes

    @property
    def masters(self) -> Set[Node]:
        """
        Return all DC/OS master ``Node``s.
        """
        return self._nodes(
            container_base_name=self._variables['MASTER_CTR'],
            num_nodes=int(self._variables['MASTERS']),
        )

    @property
    def agents(self) -> Set[Node]:
        """
        Return all DC/OS agent ``Node``s.
        """
        return self._nodes(
            container_base_name=self._variables['AGENT_CTR'],
            num_nodes=int(self._variables['AGENTS']),
        )

    @property
    def public_agents(self) -> Set[Node]:
        """
        Return all DC/OS public agent ``Node``s.
        """
        return self._nodes(
            container_base_name=self._variables['PUBLIC_AGENT_CTR'],
            num_nodes=int(self._variables['PUBLIC_AGENTS']),
        )


class DCOS_Docker_Backend:
    """
    Helper class that can create backend and customize some configuration
    options that aren't exposed with `Cluster` constructor.
    """

    def __init__(
        self,
        generate_config_path: Path,
        dcos_docker_path: Path,
        tmp_dir_path: Path=Path('/tmp')
    ) -> None:
        """
        Args:
            generate_config_path: Path to a file with DC/OS installer.
            dcos_docker_path: Path to a directory containing a checkout of
                dcos-docker.
            tmp_dir_path: Path to a directory which can be used to store
                temporary files.
        """
        self._generate_config_path = generate_config_path
        self._dcos_docker_path = dcos_docker_path
        self._tmp_dir_path = tmp_dir_path

    def create(
        self,
        masters: int,
        agents: int,
        public_agents: int,
        extra_config: Dict[str, Any],
        custom_ca_key: Optional[Path],
        log_output_live: bool,
        files_to_copy_to_installer: Dict[Path, Path],
    ) -> DCOS_Docker:
        """
        Create a DC/OS cluster using dcos-docker.

        Args:
            masters: The number of master nodes to create.
            agents: The number of agent nodes to create.
            public_agents: The number of public agent nodes to create.
            custom_ca_key: A CA key to use as the cluster's root CA key.
            extra_config: This dictionary can contain extra installation
                configuration variables to add to base configurations.
            log_output_live: If `True`, log output of subprocesses live.
                If `True`, stderr is merged into stdout in the return value.
            files_to_copy_to_installer: A mapping of host paths to paths on
                the installer node. These are files to copy from the host to
                the installer node before installing DC/OS.
        """
        return DCOS_Docker(
            masters=masters,
            agents=agents,
            public_agents=public_agents,
            extra_config=dict(extra_config or {}),
            generate_config_path=self._generate_config_path,
            dcos_docker_path=self._dcos_docker_path,
            custom_ca_key=custom_ca_key,
            log_output_live=log_output_live,
            files_to_copy_to_installer=dict(files_to_copy_to_installer or {}),
            tmp_dir_path=self._tmp_dir_path,
        )
