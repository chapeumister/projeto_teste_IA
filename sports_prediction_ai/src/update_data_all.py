import argparse
import logging
from logging.handlers import RotatingFileHandler
import sys
import os
from datetime import datetime

# --- Attempt to import main functions from collector scripts ---
# These imports assume the 'main' function is the entry point for each collector.
# If a collector uses a different function name or signature, this will need adjustment.

try:
    from data_collection import main as main_data_collection
except ImportError:
    print("WARNING: Could not import main_data_collection. It will not be available.")
    def main_data_collection(full_refresh=False, since_date=None, logger=None): pass # Placeholder

try:
    from collect_soccer_data_co_uk import main as main_collect_soccer_data_co_uk
except ImportError:
    print("WARNING: Could not import main_collect_soccer_data_co_uk. It will not be available.")
    def main_collect_soccer_data_co_uk(full_refresh=False, since_date=None, logger=None): pass

try:
    from collect_fivethirtyeight import main as main_collect_fivethirtyeight
except ImportError:
    print("WARNING: Could not import main_collect_fivethirtyeight. It will not be available.")
    def main_collect_fivethirtyeight(full_refresh=False, since_date=None, logger=None): pass

try:
    from collect_openfootball import main as main_collect_openfootball
except ImportError:
    print("WARNING: Could not import main_collect_openfootball. It will not be available.")
    def main_collect_openfootball(full_refresh=False, since_date=None, logger=None): pass

try:
    from collect_football_data_org_history import main as main_collect_football_data_org_history
except ImportError:
    print("WARNING: Could not import main_collect_football_data_org_history. It will not be available.")
    def main_collect_football_data_org_history(full_refresh=False, since_date=None, logger=None): pass

try:
    from collect_kaggle import main as main_collect_kaggle
except ImportError:
    print("WARNING: Could not import main_collect_kaggle. It will not be available.")
    def main_collect_kaggle(full_refresh=False, since_date=None, logger=None): pass

try:
    from connector_thesportsdb import main as main_connector_thesportsdb
except ImportError:
    print("WARNING: Could not import main_connector_thesportsdb. It will not be available.")
    def main_connector_thesportsdb(full_refresh=False, since_date=None, logger=None): pass


# --- Logger Setup ---
def setup_logger(name="data_updater", log_file="sports_prediction_ai/logs/data_update.log", level=logging.INFO):
    """Configures and returns a logger."""
    # Ensure log directory exists
    log_dir = os.path.dirname(log_file)
    if not os.path.exists(log_dir):
        try:
            os.makedirs(log_dir)
        except OSError as e:
            print(f"Error creating log directory {log_dir}: {e}")
            # Fallback to current directory if log dir creation fails
            log_file = os.path.basename(log_file) 
            print(f"Logging to {log_file} in current directory instead.")


    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False # Prevent duplicate logs if root logger is also configured

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')

    # Rotating File Handler
    try:
        rfh = RotatingFileHandler(log_file, maxBytes=10*1024*1024, backupCount=3, encoding='utf-8')
        rfh.setFormatter(formatter)
        logger.addHandler(rfh)
    except Exception as e:
        print(f"Error setting up RotatingFileHandler: {e}")


    # Stream Handler (console)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(formatter)
    logger.addHandler(sh)
    
    return logger

# --- Collector Mapping ---
# Maps short names to the main execution function of each collector script.
# Assumes each main function can accept (full_refresh=False, since_date=None, logger=None)
AVAILABLE_COLLECTORS = {
    "daily_live": main_data_collection,  # General daily data collection
    "soccer_data_co_uk": main_collect_soccer_data_co_uk,
    "fivethirtyeight": main_collect_fivethirtyeight,
    "openfootball": main_collect_openfootball,
    "fd_org_history": main_collect_football_data_org_history,
    "kaggle_intl": main_collect_kaggle, # Assuming kaggle collector is for international results
    "thesportsdb": main_connector_thesportsdb,
}

# --- Argument Parsing ---
def parse_arguments():
    """Defines and parses command-line arguments."""
    parser = argparse.ArgumentParser(description="Run data collection scripts for the Sports Prediction AI project.")
    parser.add_argument(
        "--sources",
        type=str,
        default="all",
        help=f"Comma-separated list of collector names to run. Available: {', '.join(AVAILABLE_COLLECTORS.keys())}. Default is 'all'."
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Indicates a full historical refresh for relevant collectors. Default is False (incremental)."
    )
    parser.add_argument(
        "--since",
        type=str,
        default=None,
        help="Start date for incremental updates (YYYY-MM-DD format). Used by some collectors."
    )
    return parser.parse_args()

# --- Main Execution Logic ---
def run_selected_collectors(args, logger):
    """Runs the selected data collectors based on parsed arguments."""
    selected_sources_str = args.sources.lower()
    collectors_to_run = []

    if selected_sources_str == "all":
        collectors_to_run = list(AVAILABLE_COLLECTORS.items())
    else:
        source_names = [name.strip() for name in selected_sources_str.split(',')]
        for name in source_names:
            if name in AVAILABLE_COLLECTORS:
                collectors_to_run.append((name, AVAILABLE_COLLECTORS[name]))
            else:
                logger.warning(f"Unknown source '{name}' specified. Skipping.")

    if not collectors_to_run:
        logger.info("No valid sources selected or 'all' specified with no available collectors. Exiting.")
        return True # No failures as nothing was run

    failed_collectors = []
    successful_collectors = []

    logger.info(f"Starting data update process. Full refresh: {args.full}, Since date: {args.since}")

    for name, collector_main_func in collectors_to_run:
        logger.info(f"--- Running collector: {name} ---")
        try:
            # Pass relevant arguments. Assumes collector main functions can handle them.
            # A more robust way would be to inspect function signature or have a standard interface.
            # For now, try passing all; individual collectors should ignore what they don't use.
            if collector_main_func.__name__ == "<lambda>" or collector_main_func.__name__ == "pass": # Skip placeholder functions
                 logger.warning(f"Collector '{name}' is a placeholder/not imported correctly. Skipping.")
                 failed_collectors.append(name) # Treat as failure if it was selected
                 continue

            # Simplistic argument passing based on common patterns
            # Ideally, collectors would have a standardized main signature or use **kwargs
            if name in ["daily_live", "fd_org_history", "thesportsdb"]: # These might use full/since
                 collector_main_func(full_refresh=args.full, since_date=args.since, logger=logger)
            else: # Others might not have specific full/since logic or are one-shot historical
                 collector_main_func(logger=logger) # Pass logger for consistency

            logger.info(f"--- Collector '{name}' finished successfully. ---")
            successful_collectors.append(name)
        except Exception as e:
            logger.error(f"--- Collector '{name}' failed: {e} ---", exc_info=True)
            failed_collectors.append(name)

    if failed_collectors:
        logger.error(f"Data update process completed with failures. Failed collectors: {', '.join(failed_collectors)}")
        return False
    else:
        logger.info("Data update process completed successfully for all selected collectors.")
        return True

# --- Script Entry Point ---
if __name__ == "__main__":
    logger = setup_logger()
    args = parse_arguments()

    logger.info("=======================================================")
    logger.info(f"Data Update Script Started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Arguments: Sources='{args.sources}', Full Refresh='{args.full}', Since Date='{args.since}'")
    logger.info("=======================================================")

    success = run_selected_collectors(args, logger)

    logger.info("=======================================================")
    if success:
        logger.info("All selected data collectors ran successfully.")
        logger.info(f"Data Update Script Finished at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=======================================================")
        sys.exit(0)
    else:
        logger.error("One or more data collectors failed.")
        logger.info(f"Data Update Script Finished with errors at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=======================================================")
        sys.exit(1)
