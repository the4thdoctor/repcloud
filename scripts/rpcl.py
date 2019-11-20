#!/usr/bin/env python
from pkg_resources import get_distribution
__version__ = get_distribution('repcloud')
import argparse
from repcloud import repack_engine

commands = [
	'create_schema',
	'drop_schema',
	'show_connections', 
	'repack_tables', 
	'prepare_repack',
	'abort_repack',
	'replay_data',
	'stop_prepare',
	'stop_repack',
	'stop_replay',
	]

if __name__ == "__main__":
	command_help = ','.join(commands)
	config_help = """Specifies the configuration to use. The configuration name may have an optional .toml suffix. The default configuration is ~/.repcloud/configuration/default.toml """
	debug_help = """Forces the debug mode with logging on stdout and log level debug."""
	debug_replay = """Starts the replay_data process as soon as the prepare_repack is finished. It applies only to prepare_repack."""
	connection_help = """	Specifies the connection to use if omitted all the connections will be used"""
	parser = argparse.ArgumentParser(description='Command line for repcloud.',  add_help=True)
	parser.add_argument('command', type=str, help=command_help)

	parser.add_argument('--config', type=str,  default='default',  required=False, help=config_help)
	parser.add_argument('--connection', type=str,  default='all',  required=False, help=connection_help)
	parser.add_argument('--debug', default=False, required=False, help=debug_help, action='store_true')
	parser.add_argument('--start-replay', default=False, required=False, help=debug_help, action='store_true')
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
