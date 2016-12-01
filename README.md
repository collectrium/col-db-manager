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
