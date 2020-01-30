#################################
# In-memory SQL-style Database
#################################
#
# Problem:
# Implement basic operations such as select, where, join, etc of a SQL-style database in memory.
#
# Instructions:
# You are provided with boilerplate classes and methods, and you are
# to fill in the implementations to complete each part.
#
# For parts 1, 2, and 3, you may (and should) make whatever changes/additions you like to the implementation of the classes,
# but please refrain from changing the function signatures
#
# Parts 4, 5, 6 are more open ended. You may decide on the public/consumer api as well as the implementation details. You may also
# create a new data set if you like.
#
# Finally, even though we have provided small example data sets, your solution should be performant on datasets many orders of magnitude larger,
# so you are encouraged to make any optimizations you believe would be useful.
#
# Solution:
# Your solution will be scored in the descending priority of correctness, memory & time complexity, and comprehensibility.


class Table:

    """An in-memory representation of a relational database table"""

    def __init__(self, name, column_names, data):
        self.name = name
        self.column_names = column_names
        self.data = data

    def select(self, projected_column_names):
        # TODO Your implementation here
        temp_data = []
        for row_num, entry in enumerate(self.data):
            row = []
            for col in projected_column_names:
                index = self.column_names.index(col)
                # if index < len(self.data[row_num]):
                row.append(entry[index])
            temp_data.append(row)
        temp_table = Table("", projected_column_names, temp_data)
        return temp_table

    def where(self, column_name, value):
        # TODO Your implementation here
        temp_data = []
        index = self.column_names.index(column_name)
        for count, entry in enumerate(self.data):
            if entry[index] == value:
                temp_data.append(entry)
        temp_table = Table("", self.column_names, temp_data)
        return temp_table

    def order_by(self, column_name):
        index = self.column_names.index(column_name)
        temp_table = Table("", self.column_names, sorted(self.data, key=lambda rows: rows[index] or 0))
        return temp_table

    def count(self, column_name):
        index = self.column_names.index(column_name)
        count = 0
        for entry in self.data:
            if entry[index] != "": # entry missing a desired value is skipped
                count += 1
        return count

    def avg(self, column_name):
        try:
            index = self.column_names.index(column_name)
            sum = 0
            count = 0
            for entry in self.data:
                if isinstance(entry[index], (int, float, complex)):
                    sum += entry[index]
                    count += 1
                elif entry[index] == "": # entry missing a desired value is skipped
                    continue
            return sum / count
        except:
            print("Can't perform avg on the given column")

    def max(self, column_name):
        try:
            index = self.column_names.index(column_name)
            max = 0
            for entry in self.data:
                if isinstance(entry[index], (int, float, complex)) and entry[index] > max:
                    max = entry[index]
                elif entry[index] == "":
                    continue
                elif isinstance(entry[index], str):
                    return
            return max
        except:
            print("Can't perform max on the given column")

    def min(self, column_name):
        try:
            index = self.column_names.index(column_name)
            min = float("inf")
            for entry in self.data:
                if isinstance(entry[index], (int, float, complex)) and entry[index] < min:
                    min = entry[index]
                elif entry[index] == "":
                    continue
                elif isinstance(entry[index], str):
                    return
            return min
        except:
            print("Can't perform min on the given column")
    def insert(self, entry):
        self.data.append(entry)

    def set(self, column_name, value):
        index = self.column_names.index(column_name)
        for entry in self.data:
            entry[index] = value

    def delete(self, original):
        for row in self.data[:]:
            original.data.remove(row)

    def __str__(self):
        result = ""
        result += ", ".join(self.column_names) + "\n"
        result += "\n".join(map(lambda row: ", ".join(map(str, row)), self.data))
        return result


class DB:
    def __init__(self):
        self.table_map = {}

    def add_table(self, table):
        self.table_map[table.name] = table

    def table(self, table_name):
        return self.table_map[table_name]

    def inner_join(self, left_table, left_table_key_name, right_table, right_table_key_name):
        # TODO Your implementation here
        temp_data = []
        left_cols = []
        right_cols = []
        for col in left_table.column_names:
            left_cols.append(left_table.name + "." + col)
        for col in right_table.column_names:
            right_cols.append(right_table.name + "." + col)

        for l_entry in left_table.data:
            for r_entry in right_table.data:
                l_index = left_table.column_names.index(left_table_key_name)
                r_index = right_table.column_names.index(right_table_key_name)
                if l_entry[l_index] == r_entry[r_index]:
                    temp_data.append(l_entry + r_entry)

        temp_table = Table("", left_cols+right_cols, temp_data)
        return temp_table

    def left_join(self, left_table, left_table_key_name, right_table, right_table_key_name):
        # TODO Your implementation here
        temp_data = []
        left_cols = []
        right_cols = []
        for col in left_table.column_names:
            left_cols.append(left_table.name + "." + col)
        for col in right_table.column_names:
            right_cols.append(right_table.name + "." + col)

        for l_entry in left_table.data:
            added = False
            for r_entry in right_table.data:
                l_index = left_table.column_names.index(left_table_key_name)
                r_index = right_table.column_names.index(right_table_key_name)
                if l_entry[l_index] == r_entry[r_index]:
                    temp_data.append(l_entry + r_entry)
                    added = True
            if not added:
                temp_data.append(l_entry + [""] * len(right_cols))
        temp_table = Table("", left_cols+right_cols, temp_data)
        return temp_table

##########################


def setup_db():
    print("Db Setup:")
    print("---------------------")
    departments_table = Table("departments", ['id', 'name'], [
        [0, 'engineering'],
        [1, 'finance'],
        [2, 'sales']
    ])
    print("departments:")
    print(departments_table, "\n")

    users_table = Table('users', ['id', 'department_id', 'name'], [
        [0, 0, 'Ian'],
        [1, 0, 'Jessica'],
        [2, 1, 'Eddie'],
        [3, 1, 'Mark']
    ])
    print("users:")
    print(users_table, "\n")

    salaries_table = Table('salaries', ['id', 'user_id', 'amount'], [
        [0, 0, 100],
        [1, 1, 150],
        [2, 1, 200],
        [3, 3, 200],
        [4, 3, 300],
        [5, 4, 400],
    ])
    print("salaries:")
    print(salaries_table, "\n")

    db = DB()
    db.add_table(users_table)
    db.add_table(departments_table)
    db.add_table(salaries_table)
    print("---------------------")

    return db


class Part1:
    """
    Implement `where` and `select`
    """

    def __init__(self, db):
        self.db = db

    def run(self):
        print("Part 1:")
        print("---------------------")
        print(
            db.table("users")
            .where("id", 1)
            .select(["id", "department_id", "name"])
        )
        print("---------------------")


class Part2:
    """
    Implement `inner_join`
    """

    def __init__(self, db):
        self.db = db

    def run(self):
        """
        Should print something like:
        users.name, departments.name
        Ian, engineering
        Jessica, engineering
        """
        print("Part 2:")
        print("---------------------")
        print(
            self.db.inner_join(db.table("users"), "department_id",
                               db.table("departments"), "id")
            .where("departments.name", "engineering")
            .select(["users.name", "departments.name"])
        )
        print("---------------------")


class Part3:
    """
    Implement `left_join`.
    """

    def __init__(self, db):
        self.db = db

    def run(self):
        """
        Should print something like:
        users.name, salaries.amount
        Ian, 100
        Jessica, 150
        Jessica, 200
        Mark, 200
        Mark, 300
        Eddie,
        """
        print("Part 3:")
        print("---------------------")
        print(
            self.db.left_join(db.table("users"), "id",
                              db.table("salaries"), "user_id")
            .select(["users.name", "salaries.amount"])
        )
        print("---------------------")


class Part4:
    """
    Ordering
    - How would you implement "order by" functionality? (e.g. show users all users ordered by their name ascending)
    - What methods or classes need to be added/edited? What would the function signature(s) be?
    - Be sure to consider performance and memory usage in your implementation
    - Please provide your usage examples below and make any implementation changes in the classes above


    id, department_id, name
    2, 1, Eddie
    0, 0, Ian
    1, 0, Jessica
    3, 1, Mark

    users.name, salaries.amount
    Eddie,
    Ian, 100
    Jessica, 150
    Jessica, 200
    Mark, 200
    Mark, 300
    """

    def __init__(self, db):
        self.db = db

    def run(self):
        # TODO Your usage examples here
        print("Part 4:")
        print("---------------------")
        print(
            db.table("users")
                .order_by("name")
        )
        print("---------------------")
        print(
            self.db.left_join(db.table("users"), "id",
                              db.table("salaries"), "user_id")
                .select(["users.name", "salaries.amount"])
                .order_by("salaries.amount")
        )
        print("---------------------")


class Part5:
    """
    Aggregations
    - How would you implement aggregation functionality such as count, avg, max, min? 
    (e.g. how many users are in the sales department? what is the average salary payment?)
    - What methods or classes need to be added/edited? What would the function signature(s) be?
    - Be sure to consider performance and memory usage in your implementation
    - Please provide your usage examples below and make any implementation changes in the classes above

    expected output:
    count:  6
    count:  5
    avg:  190.0
    max:  300
    max:  None
    min:  100
    min:  None
    """

    def __init__(self, db):
        self.db = db

    def run(self):
        # TODO Your usage examples here
        print("Part 5:")
        print("---------------------")
        print(
            "count: ", self.db.left_join(db.table("users"), "id",
                db.table("salaries"), "user_id")
                .select(["users.name", "salaries.amount"])
                .count("users.name"), "\n"
            "count: ", self.db.left_join(db.table("users"), "id",
                db.table("salaries"), "user_id")
                .select(["users.name", "salaries.amount"])
                .count("salaries.amount"), "\n"
            "avg: ", self.db.left_join(db.table("users"), "id",
                db.table("salaries"), "user_id")
                .select(["users.name", "salaries.amount"])
                .avg("salaries.amount"), "\n"
            "max: ", self.db.left_join(db.table("users"), "id",
                db.table("salaries"), "user_id")
                .select(["users.name", "salaries.amount"])
                .max("salaries.amount"), "\n"
            "max: ", self.db.table("users")
                .where("id", 1)
                .select(["id", "department_id", "name"])
                .max("name"), "\n"
            "min: ", self.db.left_join(db.table("users"), "id",
                db.table("salaries"), "user_id")
                .select(["users.name", "salaries.amount"])
                .min("salaries.amount"), "\n"
            "min: ", self.db.table("users")
                .min("name")
        )
        print("---------------------")


class Part6:
    """
    Insert/Update/Delete
    - How would you implement insert, update, and delete functionality?
    (e.g. after a Table is instantiated, how do you add/update/delete a record)
    - What methods or classes need to be added/edited? What would the function signature(s) be?
    - Be sure to consider performance and memory usage in your implementation
    - Please provide your usage examples below and make any implementation changes in the classes above

    expected output:
    id, department_id, name
    0, 0, Ian
    1, 0, Jessica
    2, 1, Eddie
    3, 1, Mark
    4, 2, Jason
    ---------------------
    id, department_id, name
    0, 0, Ian
    1, 0, Jessica
    2, 1, Eddie
    3, 1, Bob
    4, 2, Jason
    ---------------------
    id, user_id, amount
    0, 0, 100
    1, 1, 150
    2, 1, 200
    5, 4, 400
    ---------------------
    """

    def __init__(self, db):
        self.db = db

    def run(self):
        # TODO Your usage examples here
        print("Part 6:")
        print("---------------------")
        # INSERT
        self.db.table("users").insert([4, 2, 'Jason'])
        print(
            self.db.table("users")
        )
        print("---------------------")
        # UPDATE
        self.db.table("users").where("id", 3).set("name", "Bob")
        print(
            self.db.table("users")
        )
        print("---------------------")
        # DELETE
        self.db.table("salaries").where("user_id", 3).delete(self.db.table("salaries")) # remove the row where user_id = 3 from the table "salaries"
        print(self.db.table("salaries"))
        print("---------------------")

if __name__ == "__main__":
    db = setup_db()

    part1 = Part1(db)
    part1.run()

    part2 = Part2(db)
    part2.run()

    part3 = Part3(db)
    part3.run()

    part4 = Part4(db)
    part4.run()

    part5 = Part5(db)
    part5.run()

    part6 = Part6(db)
    part6.run()
