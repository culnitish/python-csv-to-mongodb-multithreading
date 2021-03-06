import csv
import random
from time import time
from decimal import Decimal
from faker import Faker
from faker.providers import BaseProvider
from configparser import ConfigParser
config = ConfigParser()
config.read('../../config.ini')

# RECORD_COUNT = 10000
# NO_OF_CSV_FILES =7
fake = Faker()


# Our custom provider inherits from the BaseProvider
class DepartmentProvider(BaseProvider):
    def department(self):
        departments = [
            'Computers', 'IT', 'Mechanical', 'Electrical', 'E&TC', 'Civil',
            'Chemical', 'Petroleum', 'Aviation', 'Metallury'
        ]

        # We select a random department from the list and return it
        return random.choice(departments)


def create_csv_file(noOfCsvFiles,noOfRecords):
    for i in range(noOfCsvFiles):
        fileName = 'student_' + str(i + 1) + '.csv'
        with open('StudentData/' + fileName, 'w', newline='') as csvfile:
            fieldnames = [
                "registration_id", "student_name", "date_of_birth", "email",
                "address", "department", "sub1", "sub2", "sub3", "sub4", "sub5"
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            for i in range(noOfRecords):
                writer.writerow({
                    'registration_id':
                    fake.random_int(min=101, max=8978767),
                    'student_name':
                    fake.name(),
                    'date_of_birth':
                    fake.date_between(start_date='-26y', end_date='-22y'),
                    'email':
                    fake.email(),
                    'address':
                    fake.street_address(),
                    'department':
                    fake.department(),
                    'sub1':
                    fake.random_int(min=10, max=100),
                    'sub2':
                    fake.random_int(min=10, max=100),
                    'sub3':
                    fake.random_int(min=10, max=100),
                    'sub4':
                    fake.random_int(min=10, max=100),
                    'sub5':
                    fake.random_int(min=10, max=100)
                })


if __name__ == '__main__':
    fake.add_provider(DepartmentProvider)
    start = time()
    noOfCsvFiles = int(config['CSVGENERATE']['NO_OF_CSV_FILES'])
    noOfRecords = int(config['CSVGENERATE']['RECORD_COUNT'])
    create_csv_file(noOfCsvFiles,noOfRecords)
    elapsed = time() - start
    print('created csv file time: {}'.format(elapsed))