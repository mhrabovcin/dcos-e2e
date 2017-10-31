"""
Tools for managing DC/OS cluster nodes.
"""

from ipaddress import IPv4Address
from pathlib import Path
from subprocess import PIPE, CompletedProcess, Popen
from typing import Dict, List, Optional

import paramiko

from ._common import run_subprocess


class Node:
    """
    A record of a DC/OS cluster node.
    """

    def __init__(self, ip_address: IPv4Address, ssh_key_path: Path) -> None:
        """
        Args:
            ip_address: The IP address of the node.
            ssh_key_path: The path to an SSH key which can be used to SSH to
                the node as the `root` user.

        Attributes:
            ip_address: The IP address of the node.
        """
        self.ip_address = ip_address
        self._ssh_key_path = ssh_key_path

    def compose_ssh_command(
        self,
        args: List[str],
        env: Optional[Dict]=None,
    ) -> List[str]:
        """
        Run the specified command on the given host using SSH.

        Args:
            args: The command to run on the node.
            env: Environment variables to be set on the node before running
                    the command. A mapping of environment variable names to
                    values.

        Returns:
            Full SSH command to be run (SSH arguments + environment variables +
            other arguments).
        """
        env = dict(env or {})

        command = []

        for key, value in env.items():
            export = "export {key}='{value}'".format(key=key, value=value)
            command.append(export)
            command.append('&&')

        command += args

        ssh_args = [
            'ssh',
            # Suppress warnings.
            # In particular, we don't care about remote host identification
            # changes.
            '-q',
            # The node may be an unknown host.
            '-o',
            'StrictHostKeyChecking=no',
            # Use an SSH key which is authorized.
            '-i',
            str(self._ssh_key_path),
            # Run commands as the root user.
            '-l',
            'root',
            # Bypass password checking.
            '-o',
            'PreferredAuthentications=publickey',
            str(self.ip_address),
        ] + command

        return ssh_args

    def run_as_root(
        self,
        args: List[str],
        log_output_live: bool=False,
        env: Optional[Dict]=None,
    ) -> CompletedProcess:
        """
        Run a command on this node as `root`.

        Args:
            args: The command to run on the node.
            log_output_live: If `True`, log output live. If `True`, stderr is
                merged into stdout in the return value.
            env: Environment variables to be set on the node before running
                the command. A mapping of environment variable names to
                values.

        Returns:
            The representation of the finished process.

        Raises:
            CalledProcessError: The process exited with a non-zero code.
        """
        ssh_args = self.compose_ssh_command(args, env)

        return run_subprocess(args=ssh_args, log_output_live=log_output_live)

    def popen_as_root(self, args: List[str],
                      env: Optional[Dict]=None) -> Popen:
        """
        Open a pipe to a command run on a node as `root`.

        Args:
            args: The command to run on the node.
            env: Environment variables to be set on the node before running
                the command. A mapping of environment variable names to
                values.

        Returns:
            The pipe object attached to the specified process.
        """
        ssh_args = self.compose_ssh_command(args, env)

        process = Popen(args=ssh_args, stdout=PIPE, stderr=PIPE)

        return process

    def scp(self, source_path: Path, dst_path: Path) -> None:
        assert source_path.exists()
        scp_cmd = [
            'scp',
            '-q',
            '-o', 'IdentitiesOnly=yes',
            '-o', 'StrictHostKeyChecking=no',
            '-i', str(node._ssh_key_path),
            '-o', 'PreferredAuthentications=publickey',
            str(source.absolute()),
            user + '@' + str(node.ip_address) + ":" + str(dest.absolute()),
        ]

        run_subprocess(scp_cmd, log_output_live=True)

    def scp2(self, source: Path, dest: Path, user: str='root') -> None:
        assert source.exists()

        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(
            str(node.ip_address),
            username=user,
            key_filename=str(node._ssh_key_path)
        )

        with closing(Write(ssh_client.get_transport(), '.')) as scp:
            scp.send_file(
                local_filename=str(source),
                remote_filename=str(dest),
            )
