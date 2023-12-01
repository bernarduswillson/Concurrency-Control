from collections import defaultdict
from colorama import Fore

"""
TRANSACTION
"""
class Transaction:
    def __init__(self):
        self.operations = []
    
    # parse input string into operations
    def parse_input(self, input_str):
        operations = input_str.split(';')
        for op in operations:
            if op[0] == 'R' or op[0] == 'W':
                op_tokens = op.split('(')
                if len(op_tokens) == 2:
                    op_type = op_tokens[0][0]
                    op_transaction = int(op_tokens[0][1:])
                    op_item = op_tokens[1][:-1]
                    operation = Operation(op_type, op_transaction, op_item)
                    self.operations.append(operation)
            elif op[0] == 'C':
                op_type = op[0]
                op_transaction = int(op[1:])
                operation = Operation(op_type, op_transaction, "")
                self.operations.append(operation)

"""
OPERATION
"""
class Operation:
    # example: R1(A)
    # type = R, transaction = 1, item = A
    def __init__(self, type, transaction, item):
        self.type = type
        self.transaction = transaction
        self.item = item


"""
SCHEDULER
"""
class Scheduler:
    def __init__(self, transaction):
        self.transaction = transaction
        self.timestamps = defaultdict(lambda: defaultdict(int))
        self.final_schedule = []\

    # generate final schedule
    def generate_final_schedule(self):
        for operation in self.transaction.operations:
            if operation.type == 'R':
                self.read(operation)
            elif operation.type == 'W':
                self.write(operation)
            elif operation.type == 'C':
                self.commit(operation)
            else:
                raise Exception(f"Invalid task {operation}")
        return self.final_schedule

    # read operation
    def read(self, operation):
        type = operation.type
        transaction = operation.transaction
        item = operation.item
        if self.timestamps[item]['W'] > transaction:
            self.timestamps[item]['R'] = self.timestamps[item]['R'] + 1
            print(Fore.RED + f"{type}{transaction}({item}) is aborted due to write-read conflict on item {item}" + Fore.RESET)
        else:
            self.timestamps[item]['R'] = max(self.timestamps[item]['R'], transaction)
            print(Fore.GREEN + f"{type}{transaction}({item}) is read successfully" + Fore.RESET)
        print("TS(" + item + ")=(" + str(self.timestamps[item]['R']) + "," + str(self.timestamps[item]['W']) + ")")
        self.final_schedule.append(f"R{transaction}({item})")

    # write operation
    def write(self, operation):
        type = operation.type
        transaction = operation.transaction
        item = operation.item
        if self.timestamps[item]['R'] > transaction:
            self.timestamps[item]['W'] = self.timestamps[item]['W'] + 1
            print(Fore.RED + f"{type}{transaction}({item}) is aborted due to write-write conflict on item {item}" + Fore.RESET)
        else:
            self.timestamps[item]['W'] = max(self.timestamps[item]['R'], transaction)
            print(Fore.GREEN + f"{type}{transaction}({item}) is written successfully" + Fore.RESET)
        print("TS(" + item + ")=(" + str(self.timestamps[item]['R']) + "," + str(self.timestamps[item]['W']) + ")")
        self.final_schedule.append(f"W{transaction}({item})")

    # commit operation
    def commit(self, operation):
        transaction = operation.transaction
        self.final_schedule.append(f"C{transaction}")


if __name__ == "__main__":
    print(Fore.GREEN + "Welcome to the MVCC Scheduler!" + Fore.RESET)
    print(Fore.WHITE + "Enter the schedule in the following format: R1(A);W1(A);C1;R2(A);W2(A);C2; ..." + Fore.RESET)
    input_str = input("Enter the schedule: ")
    t = Transaction()
    t.parse_input(input_str)

    s = Scheduler(t)
    final_schedule = s.generate_final_schedule()