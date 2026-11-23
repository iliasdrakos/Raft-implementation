# Raft KV: Κατανεμημένο Key-Value Store με τον αλγόριθμο Raft

**Πτυχιακή εργασία:** Σχεδίαση και Υλοποίηση Κατανεμημένου Key-Value Store Ανθεκτικού σε Σφάλματα με τον Αλγόριθμο Συναίνεσης Raft σε Java
**Φοιτητής:** Δράκος Ηλίας
**Επιβλέπων:** Πλατής Νικόλαος
**Συνεπιβλέπων:** Ζέστας Ορέστης
**Τμήμα:** Τμήμα Πληροφορικής και Τηλεπικοινωνιών, Πανεπιστήμιο Πελοποννήσου

Ένα cluster από 3 κόμβους συμφωνεί σε μια κοινή σειρά εντολών (`PUT`, `DEL`, `INCR`) με τον αλγόριθμο συναίνεσης Raft. Κάθε κόμβος εφαρμόζει τις εντολές με την ίδια σειρά σε ένα τοπικό key-value store, άρα όλοι καταλήγουν με τα ίδια δεδομένα. Το σύστημα συνεχίζει να λειτουργεί όσο λειτουργεί η πλειοψηφία των κόμβων. Οι αναγνώσεις (`GET`) είναι **linearizable**: ένας client δεν βλέπει ποτέ παλιά τιμή, ακόμα και όταν πέφτει ο leader ή χάνονται μηνύματα. Αυτό ελέγχεται με δικό μας client που καταγράφει ιστορικό και με checker linearizability.

---

## Πρόοδος

| Λειτουργία | Κατάσταση |
|---|---|
| Επικοινωνία κόμβων με gRPC / Protobuf (`RequestVote`, `PreVote`, `AppendEntries`, `ClientCommand`) | Ολοκληρώθηκε |
| Leader election (terms, τυχαίο election timeout, πλειοψηφία) | Ολοκληρώθηκε |
| Heartbeats και failover όταν πέσει ο leader | Ολοκληρώθηκε |
| Αποθήκευση `currentTerm` / `votedFor` στον δίσκο (ατομική εγγραφή με fsync) | Ολοκληρώθηκε |
| Log replication με consistency check και επίλυση συγκρούσεων | Ολοκληρώθηκε |
| Commit με πλειοψηφία, μόνο για entries του τρέχοντος term (NOOP στην εκλογή) | Ολοκληρώθηκε |
| Election restriction (ψήφος μόνο σε candidate με ενημερωμένο log) | Ολοκληρώθηκε |
| Key-value store ως replicated state machine (`PUT`, `DEL`, `INCR`, `GET`) | Ολοκληρώθηκε |
| Αποθήκευση του log στον δίσκο (`log.bin`) και ανάκτηση μετά από crash | Ολοκληρώθηκε |
| Ασύγχρονο AppendEntries, ένα αίτημα τη φορά ανά follower | Ολοκληρώθηκε |
| Hint στην απόρριψη AppendEntries (γρήγορος συγχρονισμός follower) | Ολοκληρώθηκε |
| **Pre-Vote** (ένας απομονωμένος κόμβος δεν ανεβάζει το term) | Ολοκληρώθηκε |
| **CheckQuorum** (ο leader παραιτείται αν δεν ακούει την πλειοψηφία) | Ολοκληρώθηκε |
| **Client sessions** (`clientId` + `seq`: μια εντολή δεν εφαρμόζεται ποτέ δύο φορές) | Ολοκληρώθηκε |
| **Linearizable GET**: μέσω log και με **ReadIndex** (χωρίς εγγραφή στο log) | Ολοκληρώθηκε |
| gRPC client (`RaftClient`) με ταυτόχρονους clients και καταγραφή ιστορικού | Ολοκληρώθηκε |
| **Checker linearizability** (`LinearizabilityChecker`) | Ολοκληρώθηκε |
| Fault injection από το terminal (`FAULT isolate / drop / delay / noreply`, `CRASH`) | Ολοκληρώθηκε |
| Ρυθμιζόμενο cluster και Docker Compose 3–5 κόμβων | Επόμενο |
| Logs / CLI σε μορφή dashboard | Προγραμματισμένο |
| Integration tests, CRC στο `log.bin` | Προγραμματισμένο |
| Snapshots / log compaction, benchmarks 3/5/7 κόμβων | Προγραμματισμένο |

---

## Πώς λειτουργεί

Κάθε κόμβος βρίσκεται κάθε στιγμή σε μία από τρεις καταστάσεις:

![alt text](<diagram.png>)

**Η ροή μιας εγγραφής** (π.χ. `PUT x 5`):

1. Ο client στέλνει την εντολή, μαζί με το `clientId` και το `seq` του, στον **leader**. Ένας follower απαντά «not the leader» και λέει ποιος είναι ο leader.
2. Ο leader την προσθέτει στο log του και τη γράφει στον δίσκο. Η εντολή **δεν** έχει γίνει ακόμα commit.
3. Τη στέλνει αμέσως στους followers (`AppendEntries`), χωρίς να περιμένει το επόμενο heartbeat.
4. Όταν την έχει η **πλειοψηφία**, ο leader τη θεωρεί committed.
5. Κάθε κόμβος εφαρμόζει τις committed εντολές, με τη σειρά του log, στο δικό του `KvStore`. Αν το ίδιο `clientId` + `seq` έχει ήδη εφαρμοστεί, δεν εφαρμόζεται ξανά (`DUPLICATE`) και επιστρέφεται το αποθηκευμένο αποτέλεσμα.
6. Ο leader απαντά στον client μόνο αφού η εντολή εφαρμοστεί.

**Η ροή μιας ανάγνωσης με ReadIndex** (`GET x`):

1. Μόνο ο leader απαντά, και μόνο αφού έχει κάνει commit ένα entry του δικού του term (το NOOP).
2. Κρατά το τρέχον `commitIndex` ως `readIndex`.
3. Στέλνει heartbeat και περιμένει απάντηση από πλειοψηφία, σε heartbeat που στάλθηκε **μετά** την αρχή της ανάγνωσης. Έτσι ξέρει ότι ήταν ακόμα leader.
4. Περιμένει να εφαρμοστούν όλα τα entries ως το `readIndex` και διαβάζει το δικό του `KvStore`.

Αν δεν ακούσει πλειοψηφία μέσα σε 1,5 s, η ανάγνωση **αποτυγχάνει** αντί να επιστρέψει πιθανώς παλιά τιμή.

### Ιδιότητες ασφάλειας και πού υλοποιούνται

| Ιδιότητα | Πώς εξασφαλίζεται | Κώδικας |
|---|---|---|
| **Election Safety**: το πολύ ένας leader ανά term | Μία ψήφος ανά term, που σώζεται στον δίσκο **πριν** την απάντηση | `RaftNode.onRequestVote`, `PersistentState.save` |
| **Log Matching**: ίδιο entry σε ίδια θέση σημαίνει ίδιο log μέχρι εκεί | Consistency check με `prevLogIndex` / `prevLogTerm` | `RaftLog.tryAppend` |
| **Leader Completeness**: κάθε νέος leader έχει όλα τα committed entries | Election restriction | `RaftLog.isUpToDate` |
| **State Machine Safety**: όλοι εφαρμόζουν τα ίδια entries με την ίδια σειρά | Commit μόνο entries του τρέχοντος term, NOOP στην εκλογή | `RaftNode.advanceCommitIndex`, `becomeLeader` |
| **Exactly-once εφαρμογή** εντολών με retry | Client sessions: `clientId` + `seq` στο `LogEntry`, έλεγχος στο apply | `KvStore.apply`, `KvStore.isDuplicate` |
| **Linearizable αναγνώσεις** | ReadIndex (ή GET μέσω log), followers δεν απαντούν τοπικά | `RaftNode.readIndexGet` |

### Επεκτάσεις πέρα από το βασικό Raft

| Επέκταση | Πρόβλημα που λύνει | Πηγή |
|---|---|---|
| Pre-Vote | Ένας κόμβος που γυρίζει μετά από απομόνωση δεν ρίχνει τον leader με μεγαλύτερο term | Ongaro, §9.6 |
| CheckQuorum | Ένας leader που δεν ακούει την πλειοψηφία (π.χ. χάνει μόνο τις απαντήσεις) παραιτείται, αντί να κολλήσει το cluster | Ongaro, §6.2 |
| Client sessions | Ένα retry μετά από timeout δεν εφαρμόζει την εντολή δεύτερη φορά | Ongaro, §6.3 |
| ReadIndex | Linearizable ανάγνωση χωρίς εγγραφή στο log (2 φορές πιο γρήγορη από GET μέσω log) | Ongaro, §6.4 |
| Hint στην απόρριψη | Ένας follower που έχει μείνει πίσω συγχρονίζεται σε ένα βήμα, αντί για ένα RPC ανά entry | Ongaro & Ousterhout, §5.3 |

---

## Έλεγχος linearizability

Ο `RaftClient` τρέχει N ταυτόχρονους clients, ο καθένας με δικό του session. Κάνουν τυχαία `PUT`/`GET` σε λίγα κλειδιά, και κάθε `PUT` γράφει μοναδική τιμή. Για κάθε πράξη γράφεται στο ιστορικό (CSV) η αρχή, το τέλος και το αποτέλεσμα: `ok`, `fail` (σίγουρα δεν έγινε) ή `unknown` (ίσως έγινε).

Ο `LinearizabilityChecker` ψάχνει, για κάθε κλειδί, μία σειρά των πράξεων που να εξηγεί όλα τα αποτελέσματα και να σέβεται τον πραγματικό χρόνο. Χρησιμοποιεί τον αλγόριθμο των Wing & Gong με την κρυφή μνήμη του Lowe. Ένα `unknown` PUT μπορεί να συνέβη οποιαδήποτε στιγμή μετά την αρχή του ή και ποτέ. Ο ίδιος ο checker ελέγχθηκε με χειροποίητα ιστορικά, με αλλοιωμένα πραγματικά ιστορικά και με 650 τυχαία ιστορικά, συγκρίνοντας με ανεξάρτητη υλοποίηση και με εξαντλητική αναζήτηση.

| Run | Σφάλματα | Πράξεις | Αποτέλεσμα |
|---|---|---|---|
| Crash του leader | CRASH | 8.659 | linearizable, 3 διπλά που έπιασαν τα sessions |
| Απώλεια μηνυμάτων | 50% στον leader και σε έναν follower | 894 | linearizable, throughput ÷10 |
| Τρία σφάλματα του leader σε 30 s | CRASH, isolate, noreply | 1.155 | linearizable, 3 αλλαγές leader |
| ReadIndex με απομονωμένο leader | CRASH, isolate | 603 | linearizable· ο απομονωμένος leader απέρριψε τα GET αντί να δώσει παλιά τιμή |
| **Αρνητικό τεστ**: GET τοπικά από οποιονδήποτε κόμβο | κανένα | 11.746 | **VIOLATION** και στα 3 κλειδιά, 17% αποδεδειγμένα παλιές αναγνώσεις |

Το αρνητικό τεστ δείχνει ότι η διάταξη μέτρησης (client, ιστορικό, checker) πράγματι πιάνει ένα πραγματικό λάθος. Λεπτομέρειες, χρονολόγια και logs βρίσκονται στην αναφορά της πτυχιακής.

---

## Τεχνολογίες

- **Java 21**, **Maven**
- **gRPC 1.68.1** και **Protocol Buffers 3.25.5** για την επικοινωνία κόμβων και clients
- **JUnit 5** για τα tests

## Δομή του project

```
raft-kv/
├── pom.xml
├── src/main/proto/raft.proto            το συμβόλαιο: μηνύματα και RPC (κόμβοι και clients)
├── src/main/java/gr/uop/ds/raft/
│   ├── Raft.java                        main: ξεκινάει gRPC server + κόμβο, διαβάζει εντολές από το terminal
│   ├── RaftServiceImpl.java             δέχεται τα RPC και τα προωθεί στον RaftNode
│   ├── RaftNode.java                    ο αλγόριθμος: εκλογές, Pre-Vote, CheckQuorum, replication, commit, ReadIndex
│   ├── RaftLog.java                     το log και οι κανόνες του (consistency check, συγκρούσεις)
│   ├── LogStorage.java                  αποθήκευση του log στον δίσκο (log.bin)
│   ├── PersistentState.java             αποθήκευση currentTerm / votedFor (tmp + fsync + atomic move)
│   ├── KvStore.java                     η state machine: key-value store με client sessions
│   ├── FaultInjector.java               fault injection σε επίπεδο gRPC (isolate, drop, delay, noreply)
│   ├── NodeLogger.java                  logs με χρόνο, term, ρόλο και κατηγορία
│   ├── Stats.java                       μετρήσεις για την εντολή STATS
│   ├── RaftClient.java                  ξεχωριστός gRPC client: ταυτόχρονοι clients, ιστορικό CSV
│   └── LinearizabilityChecker.java      checker linearizability (Wing & Gong + Lowe)
└── src/test/java/gr/uop/ds/raft/
    ├── RaftLogTest.java                 14 tests
    ├── KvStoreTest.java                 6 tests
    └── LogStorageTest.java              3 tests
```

Κάθε κόμβος κρατά την κατάστασή του στο `data/<nodeId>/` (`state.txt`, `log.bin`). Για καθαρή εκκίνηση: `rm -rf data`.

---

## Εκτέλεση

**Build και tests:**

```bash
mvn compile
mvn test
```

**Cluster 3 κόμβων:** τρία ξεχωριστά terminals.

```bash
mvn -q exec:java "-Dexec.mainClass=gr.uop.ds.raft.Raft" "-Dexec.args=node1" "-Draft.heartbeat=100"
mvn -q exec:java "-Dexec.mainClass=gr.uop.ds.raft.Raft" "-Dexec.args=node2" "-Draft.heartbeat=100"
mvn -q exec:java "-Dexec.mainClass=gr.uop.ds.raft.Raft" "-Dexec.args=node3" "-Draft.heartbeat=100"
```

Για να κρατήσεις τα logs σε αρχείο:  (Προαιρετικό αλλά βοηθητικό)
1. Πρόσθεσε έναν φάκελο στο project ονόματι 'logs' (mkdir logs μέσα στο φάκελο του Project)
2. πρόσθεσε `"-Draft.color=false" 2>&1 | tee logs/node1-log.log` δεξιά στην ετνολή που φαίνεται παραπάνω

### Διακόπτες

Κάθε μηχανισμός μπορεί να κλείσει, ώστε να φαίνεται στα πειράματα τι ακριβώς προσφέρει. Οι διακόπτες δίνονται στην εκκίνηση με `-D...`, π.χ. `"-Draft.checkQuorum=false"`. Η γραμμή `timing:` στην αρχή του log δείχνει τι ισχύει, π.χ.:

```
timing: heartbeat=100 ms, election timeout=1500-3000 ms, pre-vote=on, check-quorum=on, read-index=on, unsafe-reads=false
```

Αν ένας διακόπτης γραφτεί λάθος (π.χ. `checkQuorm`), αγνοείται χωρίς μήνυμα. Γι' αυτό η γραμμή `timing:` αξίζει έλεγχο πριν από κάθε πείραμα.

**Μηχανισμοί του αλγορίθμου** (στους κόμβους):

| Διακόπτης | Default | Ανοιχτός (`true`) | Κλειστός (`false`) | Τι φάνηκε στα πειράματα |
|---|---|---|---|---|
| `raft.prevote` | `true` | Πριν από εκλογή, ο κόμβος ρωτά πρώτα αν θα κέρδιζε. Ανεβάζει term μόνο αν πάρει πλειοψηφία. | Κάθε election timeout ανεβάζει το term. Ένας απομονωμένος κόμβος ανεβάζει συνέχεια term και, όταν επιστρέψει, ρίχνει τον leader. | Απομονωμένος κόμβος: 20 αποτυχημένα pre-votes σε 45 s, term σταθερό. Αλλάζει και ενώ τρέχει ο κόμβος, με `PREVOTE on/off`. |
| `raft.checkQuorum` | `true` | Ο leader παραιτείται αν δεν ακούσει την πλειοψηφία για `raft.electionMin` ms (1,5 s). | Ο leader μένει leader όσο στέλνει heartbeats, ακόμα κι αν δεν του απαντά κανείς. | Με `FAULT noreply` στον leader: χωρίς CheckQuorum το cluster κόλλησε για πάντα (91 s, 0 εκλογές). Με CheckQuorum: παραίτηση σε 1,5 s, νέος leader σε 4,2 s. |
| `raft.readIndex` | `true` | Το `GET` απαντιέται με ReadIndex, χωρίς εγγραφή στο log. | Το `GET` γράφεται στο log σαν εντολή και απαντιέται όταν γίνει commit. | Και τα δύο linearizable. ReadIndex: p50 3 ms, 250 GET/s, 0 εγγραφές στο log. Μέσω log: p50 6 ms, 125 GET/s, +500 εγγραφές ανά 500 GET. |
| `raft.unsafeReads` | `false` | **Σκόπιμα λάθος:** κάθε κόμβος, και οι followers, απαντά στο `GET` από την τοπική του κατάσταση. | Κανονική λειτουργία: απαντά μόνο ο leader. | **Μόνο για το αρνητικό τεστ.** Με `client.readAnyNode`: VIOLATION, 17% αποδεδειγμένα παλιές αναγνώσεις. Ποτέ ανοιχτός σε κανονική χρήση. |

**Χρόνοι** (σε ms, στους κόμβους):

| Διακόπτης | Default | Τι ορίζει | Σημείωση |
|---|---|---|---|
| `raft.heartbeat` | `500` | Κάθε πόσο ο leader στέλνει heartbeat | Στα πειράματα 100. Μικρότερο σημαίνει πιο γρήγορη ανίχνευση και μικρότερη ουρά, αλλά περισσότερα μηνύματα. |
| `raft.electionMin` / `raft.electionMax` | `1500` / `3000` | Όρια του τυχαίου election timeout | Πρέπει να είναι πολύ μεγαλύτερα από το heartbeat και τον χρόνο ενός RPC (Raft §5.6). |

**Logs** (στους κόμβους):

| Διακόπτης | Default | Τι κάνει |
|---|---|---|
| `raft.color` | `true` | Χρώματα ανά κατηγορία. Με `false` για logs σε αρχείο (`tee`). |
| `raft.verbose` | `false` | Περισσότερες λεπτομέρειες. Αλλάζει και με `VERBOSE on/off`. |

**Client** (στον `RaftClient`):

| Διακόπτης | Default | Ανοιχτός | Κλειστός |
|---|---|---|---|
| `client.readAnyNode` | `false` | Κάθε `GET` πάει σε τυχαίο κόμβο. Τα `PUT` πάνε πάντα στον leader. | Όλα πάνε στον leader (ακολουθεί το `leaderId` του «not the leader»). |

**Συνδυασμοί που χρησιμοποιήθηκαν στον έλεγχο linearizability:**

| Κόμβοι | Client | Τι ελέγχει | Αποτέλεσμα |
|---|---|---|---|
| defaults | defaults | Κανονική λειτουργία | linearizable |
| defaults | `readAnyNode=true` | Ότι οι followers δεν απαντούν τοπικά | linearizable |
| `readIndex=false` | `readAnyNode=true` | GET μέσω log | linearizable |
| `unsafeReads=true` | defaults | Τοπική ανάγνωση μόνο στον leader | linearizable όσο ο leader είναι σταθερός, άρα **δεν** αρκεί ως αρνητικό τεστ |
| `unsafeReads=true` | `readAnyNode=true` | **Αρνητικό τεστ** | **VIOLATION** |

**Εντολές στο terminal κάθε κόμβου:**

| Εντολή | Τι κάνει |
|---|---|
| `PUT key value` | Αποθηκεύει τιμή μέσω Raft. Σε follower προωθείται στον leader. |
| `DEL key` | Σβήνει το κλειδί μέσω Raft |
| `INCR key` | Αυξάνει έναν αριθμό κατά 1 (δείχνει αν μια εντολή εφαρμόστηκε δύο φορές) |
| `RETRY` | Ξαναστέλνει την τελευταία εντολή με το **ίδιο** `seq` (δοκιμή των sessions) |
| `GET key` | Linearizable ανάγνωση (ReadIndex ή μέσω log) |
| `LGET key` | Τοπική ανάγνωση: γρήγορη, αλλά μπορεί να είναι παλιά |
| `STATUS` / `LOG` | Κατάσταση του κόμβου / περιεχόμενο του log |
| `STATS [reset]` | Μετρήσεις του κόμβου: εκλογές, παραιτήσεις, χρόνος χωρίς leader, latency του commit |
| `BENCH [n]` / `BENCHGET [n]` | n εγγραφές / n εγγραφές και αναγνώσεις στη σειρά, με latency και throughput |
| `FAULT isolate [s]` | Απομόνωση του κόμβου (όλα τα RPC προς και από αυτόν χάνονται) |
| `FAULT drop N` | Χάνεται το N% των RPC |
| `FAULT delay MS` | Καθυστέρηση κάθε RPC που φτάνει στον κόμβο |
| `FAULT noreply` | Τα RPC του κόμβου φτάνουν, αλλά οι απαντήσεις προς αυτόν χάνονται |
| `FAULT off` | Τέλος των σφαλμάτων |
| `CRASH` | Άμεσος τερματισμός της διεργασίας (σαν διακοπή ρεύματος) |
| `PREVOTE on/off`, `VERBOSE on/off`, `HELP` | |

**Client και έλεγχος linearizability:**

```bash
# 5 clients, 30 s, κλειδιά k0..k2, ιστορικό στο logs/history.csv
mvn -q exec:java "-Dexec.mainClass=gr.uop.ds.raft.RaftClient" "-Dexec.args=5 30 3 logs/history.csv"

# έλεγχος του ιστορικού (exit code 0 = linearizable, 1 = παραβίαση)
java -cp target/classes gr.uop.ds.raft.LinearizabilityChecker logs/history.csv
```

Με `"-Dclient.readAnyNode=true"` κάθε GET πηγαίνει σε τυχαίο κόμβο και όχι στον leader. Όταν οι κόμβοι τρέχουν με `-Draft.unsafeReads=true`, αυτό είναι το αρνητικό τεστ, όπου ο checker πρέπει να βρει παραβιάσεις. Κατά τη διάρκεια του run μπορείς να δώσεις `CRASH` ή `FAULT ...` στα terminals των κόμβων.

---

## Γνωστοί περιορισμοί (τρέχουσα φάση)

- Το cluster είναι σταθερό στον κώδικα: 3 κόμβοι σε `localhost`, θύρες 50051–50053.
- Δεν υπάρχουν snapshots / log compaction: το `log.bin` μεγαλώνει συνέχεια και ξαναγράφεται ολόκληρο στον δίσκο.
- Το `log.bin` δεν έχει checksum (CRC), άρα μια μισογραμμένη εγγραφή δεν ανιχνεύεται.
- Οι client sessions κρατιούνται στη μνήμη και δεν λήγουν ποτέ.
- Με δύο από τους τρεις κόμβους σε σφάλμα το cluster σταματά να απαντά (διαλέγει συνέπεια αντί για διαθεσιμότητα, όπως προβλέπει ο Raft).
- Δεν υλοποιείται αλλαγή μελών του cluster.

## Επόμενα βήματα

1. Ρυθμιζόμενο cluster (διευθύνσεις από ρυθμίσεις) και Docker Compose 3 και 5 κόμβων.
2. Logs και CLI σε μορφή dashboard.
3. Integration tests (πτώση leader, διαμέριση, restart) και CRC στο `log.bin`.
4. Snapshots και log compaction.
5. Benchmarks με 3/5/7 κόμβους και αυξανόμενο φόρτο, Toxiproxy στο Docker.

---

## Βιβλιογραφία

1. D. Ongaro, J. Ousterhout, *In Search of an Understandable Consensus Algorithm (Extended Version)*, USENIX ATC 2014. https://raft.github.io/raft.pdf
2. D. Ongaro, *Consensus: Bridging Theory and Practice*, PhD thesis, Stanford University, 2014. https://github.com/ongardie/dissertation
3. M. Herlihy, J. Wing, *Linearizability: A Correctness Condition for Concurrent Objects*, ACM TOPLAS 12(3), 1990.
4. J. Wing, C. Gong, *Testing and Verifying Concurrent Objects*, Journal of Parallel and Distributed Computing 17(1–2), 1993.
5. G. Lowe, *Testing for Linearizability*, Concurrency and Computation: Practice and Experience 29(4), 2017.
6. Raft Consensus Algorithm: https://raft.github.io/
7. gRPC Java: https://grpc.io/docs/languages/java/