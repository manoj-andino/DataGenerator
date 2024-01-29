from faker import Faker
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship, sessionmaker, declarative_base
import random

Base = declarative_base()


class DynamicTable(Base):
    __tablename__ = 'dynamic_table'
    id = Column(Integer, primary_key=True)
    table_name = Column(String)
    column_name = Column(String)
    data_type = Column(String)
    foreign_key_table = Column(String, nullable=True)

    def __repr__(self):
        return f"<DynamicTable(table_name={self.table_name}, column_name={self.column_name}, data_type={self.data_type})>"


# Define your database connection
engine = create_engine('sqlite:///:memory:')

# Create tables
Base.metadata.create_all(engine)

# Create a session
Session = sessionmaker(bind=engine)
session = Session()

# Example schema as input
example_schema = [
    {'table_name': 'users', 'column_name': 'id', 'data_type': 'integer'},
    {'table_name': 'users', 'column_name': 'name', 'data_type': 'string'},
    {'table_name': 'posts', 'column_name': 'id', 'data_type': 'integer'},
    {'table_name': 'posts', 'column_name': 'title', 'data_type': 'string'},
    {'table_name': 'posts', 'column_name': 'content', 'data_type': 'string'},
    {'table_name': 'posts', 'column_name': 'author_id', 'data_type': 'integer', 'foreign_key_table': 'users'},
]

# Create dynamic tables based on the input schema
for entry in example_schema:
    table_name = entry['table_name']
    column_name = entry['column_name']
    data_type = entry['data_type']

    # Check if the table already exists, if not, create it
    if not engine.dialect.has_table(engine, table_name):
        table = type(table_name, (Base,), {'__tablename__': table_name})
        Base.metadata.create_all(engine)

    # Add entry to the DynamicTable to keep track of the schema
    dynamic_entry = DynamicTable(**entry)
    session.add(dynamic_entry)

# Commit the changes
session.commit()

# Generate mock data based on the schema
fake = Faker()


def generate_mock_data(table_name, column_name, data_type, foreign_key_table=None):
    if data_type == 'integer':
        return random.randint(1, 100)
    elif data_type == 'string':
        return fake.word()


# Fetch the schema and generate data
for entry in session.query(DynamicTable).all():
    table_name = entry.table_name
    column_name = entry.column_name
    data_type = entry.data_type
    foreign_key_table = entry.foreign_key_table

    # Generate data
    if foreign_key_table:
        # If it's a foreign key, fetch a random value from the referenced table
        referenced_table_data = session.query(DynamicTable).filter_by(table_name=foreign_key_table).all()
        foreign_key_value = random.choice(referenced_table_data).id
        print(f"Insert into {table_name} ({column_name}) values ({foreign_key_value});")
    else:
        # If it's not a foreign key, generate data based on the data type
        generated_data = generate_mock_data(table_name, column_name, data_type)
        print(f"Insert into {table_name} ({column_name}) values ({generated_data});")
