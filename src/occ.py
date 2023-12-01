from colorama import Fore

class Operation:
    def __init__(self, type, transaction, item):
        self.type = type
        self.transaction = transaction
        self.item = item

class Transaction:
    
    def __init__(self, ts):
        self.start_ts = ts
        self.validation_ts = None
        self.finish_ts = None
        self.operations = []
        self.read_items = set() 
        self.written_items = set() 

    def read(self, item):
        self.operations.append(Operation('read', self, item))
        self.read_items.add(item) 

    def write(self, item):
        self.operations.append(Operation('write', self, item))
        self.written_items.add(item)  


class Scheduler:
    def __init__(self):
        self.transactions = {}
        self.schedule = []
        self.ts = 1  

    def parse_input(self, input):
        operations = input.split(';')
        for op in operations:
            op = op.strip()
            op = op.rstrip(';')
            if len(op) == 0:
                break
            transaction = op[1]
            item = None
            op_type = op[0]
            if op_type == 'W' or op_type == 'R':
                item = op.split('(')[1].rstrip(')')
            if transaction not in self.transactions:
                self.transactions[transaction] = Transaction(self.ts)
            if item:
                if op_type == 'W' and item not in self.transactions[transaction].written_items:
                    self.transactions[transaction].written_items.add(item)
                elif op_type == 'R' and item not in self.transactions[transaction].read_items:
                    self.transactions[transaction].read_items.add(item)
            if op_type == 'W':
                self.schedule.append({"transaction": transaction, "item": item, "operation": "write"})
            elif op_type == 'R':
                self.schedule.append({"transaction": transaction, "item": item, "operation": "read"})
            elif op_type == 'C':
                self.schedule.append({"transaction": transaction, "operation": "commit"})
            else:
                raise Exception(f"Invalid task {op}")

    def execute_operation(self, task, transaction, operation, res, idx):
        if operation.type == 'write':
            item = task['item']
            transaction.write(item)
            res.append(f"W{operation.transaction}({operation.item})")
            print(f"T{operation.transaction} writes {operation.item}")
            idx += 1
        elif operation.type == 'read':
            item = task['item']
            transaction.read(item)
            res.append(f"R{operation.transaction}({operation.item})")
            print(f"T{operation.transaction} reads {operation.item}")
            idx += 1
        elif operation.type == 'commit':
            transaction.validation_ts = self.ts
            transaction.finish_ts = self.ts
            idx = self.handle_commit(task, transaction, operation, res, idx)
        return idx
    
    def handle_commit(self, task, curr_transaction, operation, res, idx):
        is_commit_allowed = True
        for key in self.transactions:
            other_transaction = self.transactions[key]
            if other_transaction != curr_transaction:
                if other_transaction.validation_ts and other_transaction.validation_ts < curr_transaction.validation_ts:
                    if other_transaction.finish_ts and other_transaction.finish_ts < curr_transaction.start_ts:
                        continue
                    elif other_transaction.finish_ts and curr_transaction.start_ts < other_transaction.finish_ts and other_transaction.finish_ts < curr_transaction.validation_ts:
                        for r1 in other_transaction.written_items:
                            if not is_commit_allowed:
                                break
                            for r2 in curr_transaction.read_items:
                                if not is_commit_allowed:
                                    break
                                if r1 == r2:
                                    conflicting_resource = r1
                                    print(f"T{task['transaction']} cannot commit, conflicting with T{key} on item {conflicting_resource}")
                                    is_commit_allowed = False
                        if not is_commit_allowed:
                            break
                    else:
                        is_commit_allowed = False
                        break

        if is_commit_allowed:
            res.append(Fore.CYAN + f"C{operation.transaction}" + Fore.RESET)
            print(f"T{operation.transaction} commits")
            idx += 1
        else:
            idx = self.handle_abort(task, curr_transaction, operation, res, idx)
        return idx
    
    def handle_abort(self, task, transaction, operation, res, idx):
        res.append(Fore.RED + f"A{operation.transaction}" + Fore.RESET)
        print(f"T{operation.transaction} aborts")
        aborted_operations = []
        for prev in range(idx):
            if self.transactions[self.schedule[prev]['transaction']] == transaction:
                aborted_operations.append(self.schedule[prev])

        self.transactions[task['transaction']] = Transaction(self.ts)

        for op in aborted_operations:
            transaction = self.transactions[op['transaction']]
            if op['operation'] == 'write':
                item = op['item']
                transaction.write(item)
                res.append(f"W{op['transaction']}({op['item']})")
                print(f"T{op['transaction']} writes {op['item']}")
            elif op['operation'] == 'read':
                item = op['item']
                transaction.read(item)
                res.append(f"R{op['transaction']}({op['item']})")
                print(f"T{op['transaction']} reads {op['item']}")
        return idx
    
    def run(self):
        res = []
        idx = 0
        while idx < len(self.schedule):
            self.ts += 1
            task = self.schedule[idx]
            transaction = self.transactions[task['transaction']]
            operation = Operation(task['operation'], task['transaction'], task.get('item'))
            idx = self.execute_operation(task, transaction, operation, res, idx)

        print("\n\nFINAL SCHEDULE:")
        for op in res:
            print(op, end="; ")
        print("\n\n")

if __name__ == '__main__':
    print(Fore.GREEN + "Optimistic Concurrency Control (OCC)" + Fore.RESET)
    print(Fore.WHITE + "Enter the schedule in the following format: R1(A);W1(A);C1;R2(A);W2(A);C2; ..." + Fore.RESET)
    input_str = input("Enter the schedule: ")
    occ = Scheduler()
    occ.parse_input(input_str)

    occ.run()
