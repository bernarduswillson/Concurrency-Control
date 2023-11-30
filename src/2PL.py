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

class Operation:
    # example: R1(A)
    # type = R, transaction = 1, item = A
    def __init__(self, type, transaction, item):
        self.type = type
        self.transaction = transaction
        self.item = item

class Lock:
    # example: R1(A)
    # type = S, transaction = 1, item = A
    def __init__(self, type, transaction, item):
        self.type = type
        self.transaction = transaction
        self.item = item

class Scheduler:
    def __init__(self, transaction):
        self.transaction = transaction
        self.locks = []
        self.timestamp = []
        self.queue = []
        self.final_schedule = []


    # FOR INITIAL SCHEUDLE
    def identify_timestamp(self):
        for operation in self.transaction.operations:
            if operation.transaction not in self.timestamp:
                self.timestamp.append(operation.transaction)

        return self.timestamp

    def acquire_shared_lock(self, operation):
        # check if there is exclusive lock on item
        for lock in self.locks:
            if lock.item == operation.item and lock.transaction != operation.transaction:
                if lock.type == 'X':
                    return False
        # acquire shared lock
        self.locks.append(Lock('S', operation.transaction, operation.item))
        print("grant-S(" + str(operation.item) + ",T" + str(operation.transaction) + ")")
        return True

    def acquire_exclusive_lock(self, operation):
        # check if there is any lock on item
        for lock in self.locks:
            if lock.item == operation.item and lock.transaction != operation.transaction:
                return False
        # acquire exclusive lock
        self.locks.append(Lock('X', operation.transaction, operation.item))
        print("grant-X(" + str(operation.item) + ",T" + str(operation.transaction) + ")")
        return True

    def release_locks(self, operation):
        flag = False

        locks_to_remove = [lock for lock in self.locks if lock.transaction == operation.transaction]

        for lock in locks_to_remove:
            print("unlock(" + str(lock.item) + ",T" + str(lock.transaction) + ")")
            self.locks.remove(lock)
            flag = True

        return flag

    def check_deadlock(self, operation):
        # check if there is any lock on item
        for lock in self.locks:
            # check if operation.transaction is older than lock.transaction
            if lock.item == operation.item and lock.transaction != operation.transaction:
                if self.timestamp.index(lock.transaction) > self.timestamp.index(operation.transaction):
                    return False
        return True

    def handle_deadlock(self, operation):

        # relase all locks of transaction
        flag = self.release_locks(operation)
        if (flag):
            print("rollback(T" + str(operation.transaction) + ")")
        # find all operations of transaction that has been executed
        executed_operations = [op for op in self.final_schedule if op.transaction == operation.transaction]
        # find all operations of transaction that in transaction
        transaction_operations = [op for op in self.transaction.operations if op.transaction == operation.transaction]
        # remove all operations of transaction from final schedule
        self.final_schedule = [op for op in self.final_schedule if op.transaction != operation.transaction]
        # remove all operations of transaction from transaction
        self.transaction.operations = [op for op in self.transaction.operations if op.transaction != operation.transaction]
        # add all executed_transactions to transaction
        for op in executed_operations:
            self.transaction.operations.append(op)
        # add all transaction_operations to transaction
        for op in transaction_operations:
            self.transaction.operations.append(op)

    def queue_operation(self, operation):
        # find all operations of transaction from transaction
        transaction_operations = [op for op in self.transaction.operations if op.transaction == operation.transaction]
        # remove all operations of transaction from transaction
        self.transaction.operations = [op for op in self.transaction.operations if op.transaction != operation.transaction]
        # add all transaction_operations to transaction
        for op in transaction_operations:
            self.queue.append(op)

    # FOR QUEUE
    def handle_deadlock_queue(self, operation):
        # relase all locks of transaction
        flag = self.release_locks(operation)
        if (flag):
            print("rollback(T" + str(operation.transaction) + ")")
        # find all operations of transaction that has been executed
        executed_operations = [op for op in self.final_schedule if op.transaction == operation.transaction]
        # find all operations of transaction that in queue
        transaction_operations = [op for op in self.queue if op.transaction == operation.transaction]
        # remove all operations of transaction from final schedule
        self.final_schedule = [op for op in self.final_schedule if op.transaction != operation.transaction]
        # remove all operations of transaction from queue
        self.queue = [op for op in self.queue if op.transaction != operation.transaction]
        # add all executed_transactions to queue
        for op in executed_operations:
            self.queue.append(op)
        # add all transaction_operations to queue
        for op in transaction_operations:
            self.queue.append(op)

    def queue_operation_queue(self, operation):
        # find all operations of transaction from queue
        transaction_operations = [op for op in self.queue if op.transaction == operation.transaction]
        # remove all operations of transaction from queue
        self.queue = [op for op in self.queue if op.transaction != operation.transaction]
        # add all transaction_operations to queue
        for op in transaction_operations:
            self.queue.append(op)


    def generate_final_schedule(self):
        # # check preconditions
        # if not self.check_preconditions():

        # identify timestamp
        self.identify_timestamp()

        while len(self.transaction.operations) > 0 or len(self.queue) > 0:
            # # print transaction operations and locks
            # print("Transaction operations: ")
            # for operation in self.transaction.operations:
            #     print(operation.type + str(operation.transaction) + "(" + operation.item + ")")
            # print("Locks: ")
            # for lock in self.locks:
            #     print(lock.type + str(lock.transaction) + "(" + lock.item + ")")

            # prioritize queue operations
            if len(self.queue) > 0:
                operation = self.queue[0]
                print("Try operation: " + operation.type + str(operation.transaction) + "(" + operation.item + ")")
                if operation.type == 'R':
                    if self.acquire_shared_lock(operation):
                        self.final_schedule.append(operation)
                        self.queue.pop(0)
                        print("")
                        continue
                    else:
                        if self.check_deadlock(operation):
                            self.handle_deadlock_queue(operation)
                            print("")
                            continue
                        else:
                            self.queue_operation_queue(operation)
                            print("queue-S(" + str(operation.item) + ",T" + str(operation.transaction) + ")")
                elif operation.type == 'W':
                    if self.acquire_exclusive_lock(operation):
                        self.final_schedule.append(operation)
                        self.queue.pop(0)
                        print("")
                        continue
                    else:
                        if self.check_deadlock(operation):
                            self.handle_deadlock_queue(operation)
                            print("")
                            continue
                        else:
                            self.queue_operation_queue(operation)
                            print("queue-X(" + str(operation.item) + ",T" + str(operation.transaction) + ")")
                elif operation.type == 'C':
                    flag = self.release_locks(operation)
                    self.final_schedule.append(operation)
                    self.queue.pop(0)
                    print("")
                    continue

            # transaction operations
            operation = self.transaction.operations[0]
            print("Try operation: " + operation.type + str(operation.transaction) + "(" + operation.item + ")")

            if operation.type == 'R':
                if self.acquire_shared_lock(operation):
                    self.final_schedule.append(operation)
                    self.transaction.operations.pop(0)
                else:
                    if self.check_deadlock(operation):
                        self.handle_deadlock(operation)
                    else:
                        self.queue_operation(operation)
                        print("queue-S(" + str(operation.item) + ",T" + str(operation.transaction) + ")")
            elif operation.type == 'W':
                if self.acquire_exclusive_lock(operation):
                    self.final_schedule.append(operation)
                    self.transaction.operations.pop(0)
                else:
                    if self.check_deadlock(operation):
                        self.handle_deadlock(operation)
                    else:
                        self.queue_operation(operation)
                        print("queue-X(" + str(operation.item) + ",T" + str(operation.transaction) + ")")
            elif operation.type == 'C':
                flag = self.release_locks(operation)
                self.final_schedule.append(operation)
                self.transaction.operations.pop(0)

            print("")

        return self.final_schedule

    def print_final_schedule(self):
        for operation in self.final_schedule:
            if operation.type == 'C':
                print(operation.type + str(operation.transaction))
            else:
                print(operation.type + str(operation.transaction) + "(" + operation.item + ")")

t = Transaction()
input_str = "R1(A);R2(B);W1(A);R1(B);W3(A);W4(B);W2(B);R1(C);C1;C2;C3;C4"
t.parse_input(input_str)

s = Scheduler(t)
final_schedule = s.generate_final_schedule()
print("Final schedule: ")
s.print_final_schedule()
