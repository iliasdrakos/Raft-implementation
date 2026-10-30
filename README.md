# Raft KV: Κατανεμημένο Key-Value Store με τον αλγόριθμο Raft

**Πτυχιακή εργασία:** Σχεδίαση και Υλοποίηση Κατανεμημένου Key-Value Store Ανθεκτικού σε Σφάλματα με τον Αλγόριθμο Συναίνεσης Raft σε Java
**Φοιτητής:** Δράκος Ηλίας
**Επιβλέπων:** Πλατής Νικόλαος
**Τμήμα:** Τμήμα Πληροφορικής και τηλεπικοινωνιών, Πανεπιστήμιο Πελοποννήσου

Ένα cluster από 3 κόμβους που συμφωνούν σε μια κοινή σειρά εντολών (`PUT`, `DEL`) με τον αλγόριθμο συναίνεσης Raft. Κάθε κόμβος εφαρμόζει τις εντολές με την ίδια σειρά σε ένα τοπικό key-value store. Έτσι όλοι οι κόμβοι καταλήγουν με τα ίδια δεδομένα, και το σύστημα συνεχίζει να λειτουργεί όταν πέσει ένας κόμβος (μειοψηφία).

---

## Πρόοδος

| Λειτουργία | Κατάσταση |
|---|---|
| Επικοινωνία κόμβων με gRPC / Protobuf (`RequestVote`, `AppendEntries`) | Ολοκληρώθηκε |
| Leader election (terms, τυχαίο election timeout, πλειοψηφία) | Ολοκληρώθηκε |
| Heartbeats και failover όταν πέσει ο leader | Ολοκληρώθηκε |
| Αποθήκευση `currentTerm` / `votedFor` στον δίσκο (ατομική εγγραφή με fsync) | Ολοκληρώθηκε |
| Log replication με consistency check και επίλυση συγκρούσεων | Ολοκληρώθηκε |
| Commit με πλειοψηφία, μόνο για entries του τρέχοντος term (NOOP στην εκλογή) | Ολοκληρώθηκε |
| Election restriction (ψήφος μόνο σε candidate με ενημερωμένο log) | Ολοκληρώθηκε |
| Key-value store ως replicated state machine (`PUT`, `DEL`, `GET`) | Ολοκληρώθηκε |
| Αποθήκευση του log στον δίσκο | Σε εξέλιξη |
| gRPC API για clients (`Put` / `Get` / `Delete`) | Επόμενο |
| Docker Compose cluster 3–5 κόμβων | Προγραμματισμένο |
| Integration tests με fault injection | Προγραμματισμένο |

---

## Πώς λειτουργεί

Κάθε κόμβος βρίσκεται κάθε στιγμή σε μία από τρεις καταστάσεις:

![alt text](<diagram.png>)

**Η ροή μιας εντολής** (π.χ. `PUT x 5`):

1. Ο client στέλνει την εντολή στον **leader**.
2. Ο leader την προσθέτει στο log του. Η εντολή **δεν** έχει γίνει ακόμα commit.
3. Με το επόμενο heartbeat (κάθε 500 ms) τη στέλνει στους followers (`AppendEntries`).
4. Όταν την έχει η **πλειοψηφία**, ο leader τη θεωρεί committed.
5. Κάθε κόμβος εφαρμόζει τις committed εντολές, με τη σειρά του log, στο δικό του `KvStore`.

### Ιδιότητες ασφάλειας και πού υλοποιούνται

| Ιδιότητα | Πώς εξασφαλίζεται | Κώδικας |
|---|---|---|
| **Election Safety**: το πολύ ένας leader ανά term | Μία ψήφος ανά term, που σώζεται στον δίσκο **πριν** την απάντηση | `RaftNode.onRequestVote`, `PersistentState.save` |
| **Log Matching**: ίδιο entry σε ίδια θέση σημαίνει ίδιο log μέχρι εκεί | Consistency check με `prevLogIndex` / `prevLogTerm` | `RaftLog.tryAppend` |
| **Leader Completeness**: κάθε νέος leader έχει όλα τα committed entries | Election restriction | `RaftLog.isUpToDate` |
| **State Machine Safety**: όλοι εφαρμόζουν τα ίδια entries με την ίδια σειρά | Commit μόνο entries του τρέχοντος term, NOOP στην εκλογή | `RaftNode.advanceCommitIndex`, `becomeLeader` |


---

## Τεχνολογίες

- **Java 21**, **Maven**
- **gRPC 1.68.1** και **Protocol Buffers 3.25.5** για την επικοινωνία των κόμβων
- **JUnit 5** για τα tests

## Δομή του project

```
raft-kv/
├── pom.xml
├── src/main/proto/raft.proto            το συμβόλαιο: μηνύματα και RPC μεταξύ κόμβων
├── src/main/java/gr/uop/ds/raft/
│   ├── Raft.java                        main: ξεκινάει gRPC server + κόμβο, διαβάζει εντολές από το terminal
│   ├── RaftServiceImpl.java             δέχεται τα RPC και τα προωθεί στον RaftNode
│   ├── RaftNode.java                    ο αλγόριθμος: εκλογές, heartbeats, replication, commit
│   ├── RaftLog.java                     το log και οι κανόνες του (consistency check, συγκρούσεις)
│   ├── PersistentState.java             αποθήκευση currentTerm / votedFor (tmp + fsync + atomic move)
│   └── KvStore.java                     η state machine: ένα key-value store
└── src/test/java/gr/uop/ds/raft/
    ├── RaftLogTest.java                 14 tests
    └── KvStoreTest.java                 6 tests
```

---

## Εκτέλεση

**Build και tests:**

```bash
mvn compile
mvn test
```

**Cluster 3 κόμβων:** τρία ξεχωριστά terminals.

```bash
mvn -q exec:java "-Dexec.mainClass=gr.uop.ds.raft.Raft" "-Dexec.args=node1"
mvn -q exec:java "-Dexec.mainClass=gr.uop.ds.raft.Raft" "-Dexec.args=node2"
mvn -q exec:java "-Dexec.mainClass=gr.uop.ds.raft.Raft" "-Dexec.args=node3"
```

Στο terminal κάθε κόμβου μπορείς να γράψεις:

| Εντολή | Τι κάνει |
|---|---|
| `PUT key value` | Αποθηκεύει τιμή. Μόνο ο leader τη δέχεται, οι followers απαντούν ποιος είναι ο leader. |
| `DEL key` | Σβήνει το κλειδί (μέσω Raft, όπως το `PUT`) |
| `GET key` | Διαβάζει από το **τοπικό** αντίγραφο του κόμβου |
---

## Γνωστοί περιορισμοί (τρέχουσα φάση)

- **Το log δεν αποθηκεύεται ακόμα στον δίσκο.** Σε restart ο κόμβος ξαναπαίρνει τα entries από τον leader. Αν πέσουν **όλοι** οι κόμβοι, τα δεδομένα χάνονται. Αυτό είναι το επόμενο βήμα.
- Ο client είναι προσωρινά το terminal κάθε κόμβου. Θα αντικατασταθεί από gRPC API.
- Το `GET` σε follower μπορεί να επιστρέψει ελαφρώς παλιά τιμή (stale read).
- Το cluster είναι σταθερό: 3 κόμβοι σε `localhost`.
- Δεν υλοποιούνται snapshots / log compaction ούτε αλλαγή μελών του cluster. Είναι πιθανές επεκτάσεις.

## Επόμενα βήματα

1. Αποθήκευση του log στον δίσκο (append + fsync, ανάκτηση μετά από crash).
2. gRPC API για clients (`Put` / `Get` / `Delete`) με ανακατεύθυνση στον leader.
3. Docker Compose για cluster 3–5 κόμβων.
4. Integration tests: πτώση leader, διαμέριση δικτύου, restart κόμβων.

---

## Βιβλιογραφία

1. D. Ongaro, J. Ousterhout, *In Search of an Understandable Consensus Algorithm (Extended Version)*, USENIX ATC 2014. https://raft.github.io/raft.pdf
2. Raft Consensus Algorithm: https://raft.github.io/
3. gRPC Java: https://grpc.io/docs/languages/java/
