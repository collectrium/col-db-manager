# Transaction & Database Manager for SQLAlchemy

Makes transaction/session management more explicit

![build status](https://travis-ci.org/collectrium/col-db-manager.svg?branch=master)

Usage:

```python
from omnia import Transaction

with Transaction() as txn:
    txn.session.add(MyModel(id=1, value=2))
    txn.session.commit()

    models = txn.session.execute(select([MyModel])).fetchall()
    print(models[0].value)  # 2

    # transaction is commited on context exit
```

Savepoint usage:


```python
from omnia import Transaction

with Transaction() as txn:
    with txn.savepoint() as sp:
        with sp.savepoint() as sp2:
            sp.session.add(MyModel(id=1, value=2))
            sp.rollback()

    models = txn.session.execute(select([MyModel])).fetchall()
    print(models)  # []

    # transaction is commited on context exit
```

About tox

Tox is a generic virtualenv management and test command line tool you can use for:

checking your package installs correctly with different Python versions and interpreters
running your tests in each of the environments, configuring your test tool of choice
acting as a frontend to Continuous Integration servers, greatly reducing boilerplate and merging CI and shell-based testing.
How to use tox:

pip install tox # (in global environment)

`tox` # install all dependencies and run tests

`tox -r` reinstall dependencies and run tests
