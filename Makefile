.PHONY: test_environment requirements data sync_data_from_s3

#################################################################################
# GLOBALS                                                                       #
#################################################################################

BUCKET = flights-a8aa5078
PROFILE = default
PROJECT_NAME = flight-delays
PYTHON_INTERPRETER = python3

#################################################################################
# COMMANDS                                                                      #
#################################################################################

## Install Python Dependencies
requirements: test_environment
	$(PYTHON_INTERPRETER) -m pip install -U pip setuptools wheel
	$(PYTHON_INTERPRETER) -m pip install -r requirements.txt

## Make Dataset
data: requirements
	$(PYTHON_INTERPRETER) src/data/make_dataset.py data/raw data/processed

## Download Data from S3
sync_data_from_s3:
ifeq (default,$(PROFILE))
	aws s3 sync s3://$(BUCKET)/ data/raw/
else
	aws s3 sync s3://$(BUCKET)/ data/raw/ --profile $(PROFILE)
endif

## Test python environment is setup correctly
test_environment:
	$(PYTHON_INTERPRETER) test_environment.py
