"""Main entry file."""
import argparse
import os

import log_paser
import logger

import paths

log = logger.get_logger(__file__)


def prepare_args():
    """Create arg parser and return args."""
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)

    parser.add_argument(
        '-f', '--file-to-parse', default='qa_ExpTester_PreInterview_Assigment (1) (1) (1).log',
        help='The file path to be parsed, default to "logfile.log".')
    parser.add_argument(
        '--figure', action='store_true',
        help='generate figures after parsing the log file.')
    parser.add_argument(
        '--clean', action='store_true', help='remove existing log and parsed result before testing')
    parser.add_argument(
        '-l', '--log-level', default='INFO', choices=['ERROR', 'WARN', 'INFO', 'DEBUG'],
        help='set the logging level, default to "INFO" level.')

    args = parser.parse_args()
    log.info(args)

    return args


def main():
    """Main entre."""
    args = prepare_args()
    paths.ensure_paths_exists()
    logger.init_logger(args.log_level, args.clean)
    if args.clean:
        if os.path.exists(paths.FIGURE_HTML):
            os.remove(paths.FIGURE_HTML)
        if os.path.exists(paths.FIGURE_PNG):
            os.remove(paths.FIGURE_PNG)
        if os.path.exists(paths.PASER_RESULT_FILE):
            os.remove(paths.PASER_RESULT_FILE)

    log_parser = log_paser.LogParser(args.file_to_parse)
    # log_parser.parse_events('2018-10-11 15:50:42.284', '2018-10-11 15:50:42.299')
    log_parser.parse_events()
    log_parser.generate_results()
    if args.figure:
        log_parser.generate_figures()
    log_parser.show_results()


if __name__ == "__main__":
    main()
