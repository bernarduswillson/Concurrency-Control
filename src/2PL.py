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
LOCK
"""
class Lock:
    # example: R1(A)
    # type = S, transaction = 1, item = A
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
        self.locks = []
        self.timestamp = []
        self.queue = []
        self.final_schedule = []
        self.result = []


    """
    FOR INITIAL TRANSACTION
    """
    def identify_timestamp(self):
        # identify transaction order
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

        # acquire shared lock, if already have shared lock, do nothing
        for lock in self.locks:
            if lock.item == operation.item and lock.transaction == operation.transaction and lock.type == 'S':
                return True
        self.locks.append(Lock('S', operation.transaction, operation.item))
        self.result.append(Fore.GREEN + "SL" + str(operation.transaction) + "(" + operation.item + ")" + Fore.RESET)
        print(Fore.GREEN + "grant-S(" + str(operation.item) + ",T" + str(operation.transaction) + ")" + Fore.RESET)

        return True

    def acquire_exclusive_lock(self, operation):
        # check if there is any lock on item
        for lock in self.locks:
            if lock.item == operation.item and lock.transaction != operation.transaction:
                return False

        # acquire exclusive lock, if already have shared lock, upgrade to exclusive lock
        flag = False
        for lock in self.locks:
            if lock.item == operation.item and lock.transaction == operation.transaction and lock.type == 'S':
                flag = True
                break
        if flag:
            self.result.append(Fore.LIGHTGREEN_EX + "UPL" + str(operation.transaction) + "(" + operation.item + ")" + Fore.RESET)
        else:
            self.result.append(Fore.LIGHTGREEN_EX + "XL" + str(operation.transaction) + "(" + operation.item + ")" + Fore.RESET)
        self.locks.append(Lock('X', operation.transaction, operation.item))
        print(Fore.LIGHTGREEN_EX + "grant-X(" + str(operation.item) + ",T" + str(operation.transaction) + ")" + Fore.RESET)

        return True

    def release_locks(self, operation):
        flag = False
        removed_lock = []
        
        # remove all locks of transaction
        locks_to_remove = [lock for lock in self.locks if lock.transaction == operation.transaction]
        for lock in locks_to_remove:
            lock_info = (lock.transaction, lock.item)
            if lock_info not in removed_lock:
                self.result.append(Fore.BLUE + "UL" + str(lock.transaction) + "(" + lock.item + ")" + Fore.RESET)
                print(Fore.BLUE + "unlock(" + str(lock.item) + ",T" + str(lock.transaction) + ")" + Fore.RESET)
                removed_lock.append(lock_info)
            self.locks.remove(lock)
            flag = True

        return flag

    def check_deadlock(self, operation):
        # check if there is any lock on item, check if opreation.transaction is the oldest transaction
        for lock in self.locks:
            if lock.item == operation.item and lock.transaction != operation.transaction and self.timestamp.index(lock.transaction) < self.timestamp.index(operation.transaction):
                return True

        return False

    def handle_deadlock(self, operation):
        # relase all locks of transaction
        flag = self.release_locks(operation)
        if (flag):
            self.result.append(Fore.RED + "A" + str(operation.transaction) + Fore.RESET)
            print(Fore.RED + "rollback(T" + str(operation.transaction) + ")" + Fore.RESET)

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
        
        if operation.type == 'R':
            print(Fore.YELLOW + "queue-S(" + str(operation.item) + ",T" + str(operation.transaction) + ")" + Fore.RESET)
        elif operation.type == 'W':
            print(Fore.YELLOW + "queue-X(" + str(operation.item) + ",T" + str(operation.transaction) + ")" + Fore.RESET)


    """
    FOR QUEUE
    """
    def handle_deadlock_queue(self, operation):
        # relase all locks of transaction
        flag = self.release_locks(operation)
        if (flag):
            self.result.append(Fore.RED + "A" + str(operation.transaction) + Fore.RESET)
            print(Fore.RED + "rollback(T" + str(operation.transaction) + ")" + Fore.RESET)

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

    
    """
    HANDLE PRECONDITIONS
    """
    def check_preconditions(self):
        # check if there is double commit
        for operation in self.transaction.operations:
            if operation.type == 'C':
                count = 0
                for op in self.transaction.operations:
                    if op.type == 'C' and op.transaction == operation.transaction:
                        count += 1
                if count > 1:
                    return "Invalid schedule: Commit more than once"

        # check if there is operation after commit
        for operation in self.transaction.operations:
            if operation.type == 'C':
                for op in self.transaction.operations:
                    if op.transaction == operation.transaction and self.transaction.operations.index(op) > self.transaction.operations.index(operation):
                        return "Invalid schedule: Operation after commit"
            
        # check if operation without commit
        for operation in self.transaction.operations:
            if operation.type != 'C':
                count = 0
                for op in self.transaction.operations:
                    if op.type == 'C' and op.transaction == operation.transaction:
                        count += 1
                if count == 0:
                    return "Invalid schedule: Operation without commit"

        # check if commit without operation
        for operation in self.transaction.operations:
            if operation.type == 'C':
                count = 0
                for op in self.transaction.operations:
                    if op.transaction == operation.transaction:
                        count += 1
                if count == 1:
                    return "Invalid schedule: Commit without operation"
        
        return True

    """
    MAIN ALGORITHM
    """
    def generate_final_schedule(self):
        # check preconditions
        if self.check_preconditions() != True:
            self.final_schedule = self.check_preconditions()
            return self.final_schedule

        # identify timestamp
        self.identify_timestamp()

        while len(self.transaction.operations) > 0 or len(self.queue) > 0:
            print("")
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
                        self.result.append("R" + str(operation.transaction) + "(" + operation.item + ")")
                        self.queue.pop(0)
                        continue
                    else:
                        if self.check_deadlock(operation):
                            self.handle_deadlock_queue(operation)
                            continue
                        else:
                            self.queue_operation_queue(operation)
                            print("")

                elif operation.type == 'W':
                    if self.acquire_exclusive_lock(operation):
                        self.final_schedule.append(operation)
                        self.result.append("W" + str(operation.transaction) + "(" + operation.item + ")")
                        self.queue.pop(0)
                        continue
                    else:
                        if self.check_deadlock(operation):
                            self.handle_deadlock_queue(operation)
                            continue
                        else:
                            self.queue_operation_queue(operation)
                            print("")

                elif operation.type == 'C':
                    flag = self.release_locks(operation)
                    self.final_schedule.append(operation)
                    self.result.append(Fore.CYAN + "C" + str(operation.transaction) + Fore.RESET)
                    self.queue.pop(0)
                    continue
    

            # transaction operations
            if len(self.transaction.operations) > 0:
                operation = self.transaction.operations[0]
                print("Try operation: " + operation.type + str(operation.transaction) + "(" + operation.item + ")")

                if operation.type == 'R':
                    if self.acquire_shared_lock(operation):
                        self.final_schedule.append(operation)
                        self.result.append("R" + str(operation.transaction) + "(" + operation.item + ")")
                        self.transaction.operations.pop(0)
                    else:
                        if self.check_deadlock(operation):
                            self.handle_deadlock(operation)
                        else:
                            self.queue_operation(operation)

                elif operation.type == 'W':
                    if self.acquire_exclusive_lock(operation):
                        self.final_schedule.append(operation)
                        self.result.append("W" + str(operation.transaction) + "(" + operation.item + ")")
                        self.transaction.operations.pop(0)
                    else:
                        if self.check_deadlock(operation):
                            self.handle_deadlock(operation)
                        else:
                            self.queue_operation(operation)

                elif operation.type == 'C':
                    flag = self.release_locks(operation)
                    self.final_schedule.append(operation)
                    self.result.append(Fore.CYAN + "C" + str(operation.transaction) + Fore.RESET)
                    self.transaction.operations.pop(0)


        return self.final_schedule


    """
    OUTPUT
    """
    def print_final_schedule(self):
        if type(self.final_schedule) == str:
            print(self.final_schedule)
            return
        for operation in self.final_schedule:
            if operation.type == 'C':
                print(operation.type + str(operation.transaction))
            else:
                print(operation.type + str(operation.transaction) + "(" + operation.item + ")")

    def print_result(self):
        for operation in self.result:
            print(operation + "; ", end="")


"""
MAIN
"""
if __name__ == "__main__":
    print(Fore.GREEN + "Two-Phase Locking (2PL)" + Fore.RESET)
    print(Fore.WHITE + "Enter the schedule in the following format: R1(A);W1(A);C1;R2(A);W2(A);C2; ..." + Fore.RESET)
    input_str = input("Enter the schedule: ")
    t = Transaction()
    t.parse_input(input_str)

    s = Scheduler(t)
    final_schedule = s.generate_final_schedule()
    print("\n\nFINAL SCHEDULE:")
    s.print_result()
    print("\n\n")