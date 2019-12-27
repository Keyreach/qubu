from qubu import Database, Column

db = Database('test.db')

with db.posts:
    db.posts.create(id = Column('integer', primary_key=True), title=Column('text'), author=Column('text'))
    
    db.posts().insert(id = 1, title = 'Fizz', author = 'John Doe')
    db.posts().insert(id = 2, title = 'Buzz', author = 'John Doe')

    result = db.posts().where(author = 'John Doe').select('title', 'id')
    for row in result.fetchall():
        print(row)

    db.posts().where(id=1).update(title='Foo')
    db.posts().delete()
