# Concurrency Control Simulator

## Description

This program implements three concurrency control mechanisms: MVCC (Multi-Version Concurrency Control), 2PL (Two-Phase Locking), and OCC (Optimistic Concurrency Control). These mechanisms are commonly used in database management systems to control access to data and ensure consistency in concurrent transactions.

### 2PL (Two-Phase Locking)
2PL is a locking mechanism that ensures serializability by using two phases: the growing phase and the shrinking phase. In the growing phase, a transaction can acquire locks but cannot release any. In the shrinking phase, a transaction can release locks but cannot acquire new ones. This mechanism helps prevent conflicts between transactions and ensures a consistent state.

### OCC (Optimistic Concurrency Control)
OCC allows transactions to proceed optimistically without acquiring locks. During the validation phase, conflicts between transactions are checked. If conflicts are detected, one or more transactions may need to be aborted. This mechanism is suitable for scenarios where conflicts are infrequent, and the system optimistically assumes that most transactions will commit without conflicts.

### MVCC (Multiversion Timestamp Ordering Concurrency Control )
MVCC allows multiple versions of a data item to exist simultaneously in the database. Each transaction gets its own snapshot of the database, ensuring that read and write operations do not interfere with each other. This mechanism enhances concurrency by allowing transactions to proceed without waiting for locks.

## How to Run

To run the program, execute the following command in the terminal:

```bash
python [filename].py
```
Replace `[filename]` with the actual name of the python file in the program.

## Valid Input

Valid input consists of a sequence of transactions, where each transaction includes read (R), write (W), or commit (C) operations. The general format for valid input is as follows:

- `C[number]`: Commit operation for a specific transaction number.
- `R[number](alphabet)`: Read operation for a specific transaction number on a data item identified by an alphabet.
- `W[number](alphabet)`: Write operation for a specific transaction number on a data item identified by an alphabet.

Here is an example of valid input:

```bash
R1(X); R2(Y); R1(Y); C1; C2
```

