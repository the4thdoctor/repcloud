#!/usr/bin/env python
from pkg_resources import get_distribution
__version__ = get_distribution('repcloud')
import argparse
from repcloud import repack_engine

commands = [
	'show_config',
	]

command_help = ','.join(commands)
config_help = """Specifies the configuration to use without the suffix yml. If  the parameter is omitted then ~/.pg_chameleon/configuration/default.yml is used"""
schema_help = """Specifies the schema within a source. If omitted all schemas for the given source are affected by the command. Requires the argument --source to be specified"""
source_help = """Specifies the source within a configuration. If omitted all sources are affected by the command."""
tables_help = """Specifies the tables within a source . If omitted all tables are affected by the command."""
logid_help = """Specifies the log id entry for displaying the error details"""
debug_help = """Forces the debug mode with logging on stdout and log level debug."""
version_help = """Displays pg_chameleon's installed  version."""
rollbar_help = """Overrides the level for messages to be sent to rolllbar. One of: "critical", "error", "warning", "info". The Default is "info" """
full_help = """When specified with run_maintenance the switch performs a vacuum full instead of a normal vacuum. """


parser = argparse.ArgumentParser(description='Command line for pg_chameleon.',  add_help=True)
parser.add_argument('command', type=str, help=command_help)
parser.add_argument('--config', type=str,  default='default',  required=False, help=config_help)
parser.add_argument('--schema', type=str,  default='*',  required=False, help=schema_help)
parser.add_argument('--source', type=str,  default='*',  required=False, help=source_help)
parser.add_argument('--tables', type=str,  default='*',  required=False, help=tables_help)
parser.add_argument('--logid', type=str,  default='*',  required=False, help=logid_help)
parser.add_argument('--debug', default=False, required=False, help=debug_help, action='store_true')
parser.add_argument('--version', action='version', help=version_help,version='{version}'.format(version=__version__))
parser.add_argument('--rollbar-level', type=str, default="info", required=False, help=rollbar_help)
parser.add_argument('--full', default=False, required=False, help=full_help, action='store_true')
args = parser.parse_args()


repack = repack_engine(args)
if args.debug:
	getattr(repack, args.command)()
else:
	try:
		getattr(repack, args.command)()
	except AttributeError:
		print("ERROR - Invalid command" )
		print(command_help)
