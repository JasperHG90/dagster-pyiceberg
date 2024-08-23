alias s := setup
alias p := pre_commit

# Install python dependencies
install:
  poetry install

# Install pre-commit hooks
pre_commit_setup:
  poetry run pre-commit install

# Install python dependencies and pre-commit hooks
setup: install pre_commit_setup

# Run pre-commit
pre_commit:
  poetry run pre-commit run -a
