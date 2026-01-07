package eventSourcing

import akka.actor.{ActorLogging, ActorSystem, Props}
import akka.persistence.{PersistentActor, SaveSnapshotFailure, SaveSnapshotSuccess, SnapshotOffer}

object Snapshots extends App {
/*
/system                           (system guardian)
  └── akka.persistence.snapshot-store  (SnapshotStore actor instance)
  └── akka.persistence.journal         (Journal actor instance)
   journal actor is the one that:
reads stored events from the journal storage (DB/Cassandra/file/etc)
sends them back to your persistent actor during recovery

/user
  └── accountantSupervisor
        └── simpleAccountant           (your PersistentActor)

                ActorSystem
         ┌─────────────────────────┐
         │                         │
         │   /system               │
         │    ├─ journal (actor)   │
         │    └─ snapshotStore     │  <--- "snapshot actor"
         │                         │
         │   /user                 │
         │    └─ accountantSupervisor
         │         └─ simpleAccountant (PersistentActor)
         └─────────────────────────┘

Caller/Traffic
    |
    v
/accountantSupervisor  (forwards)
    |
    v
/simpleAccountant (PersistentActor)
    |
    |  saveSnapshot(state)
    v
/system/akka.persistence.snapshot-store
    |
    | writes snapshot to disk/DB
    v
(system snapshot storage)
    |
    | reply back
    v
/simpleAccountant mailbox:
    - SaveSnapshotSuccess(meta)
       OR
    - SaveSnapshotFailure(meta, ex)

                     ActorSystem
   ┌─────────────────────────────────┐
   │ /system                         │
   │   ├── snapshot-store  (actor)   │
   │   └── journal         (actor)   │  <-- sends replayed events
   │                                 │
   │ /user                           │
   │   └── yourPersistentActor       │
   └─────────────────────────────────┘

Recovery:
yourPersistentActor  ->  journal actor : ReplayMessages(persistenceId, fromSeq, toSeq)
journal actor         ->  yourPersistentActor : Event(seq=fromSeq)
journal actor         ->  yourPersistentActor : Event(seq=fromSeq+1)
...
journal actor         ->  yourPersistentActor : RecoveryComplete
TODO
 Your persistent actor owns snapshot semantics
 SnapshotStore plugin actor does IO
 Snapshotter trait is convenience wrapper to send protocol messages

┌──────────────────────┐
│ ActorSystem restart  │
└──────────┬───────────┘
           │
           ▼
┌───────────────────────────────┐
│ new MyPersistentActor instance│
└──────────┬────────────────────┘
           │ aroundPreStart()
           ▼
┌─────────────────────────────────────────────┐
│ Eventsourced.requestRecoveryPermit()         │
└──────────┬──────────────────────────────────┘
           │ RequestRecoveryPermit
           ▼
┌──────────────────────────┐
│ RecoveryPermitter actor  │
└──────────┬───────────────┘
           │ RecoveryPermitGranted
           ▼
┌─────────────────────────────────────────────┐
│ Eventsourced.startRecovery()                 │
│ state = recoveryStarted                     │
└──────────┬──────────────────────────────────┘
           │ LoadSnapshot(persistenceId)
           ▼
┌──────────────────────────┐
│ SnapshotStore plugin     │
│ (IO read)                │
└──────────┬───────────────┘
           │ LoadSnapshotResult
           ▼
┌─────────────────────────────────────────────┐
│ Eventsourced                                 │
│ fabricate SnapshotOffer                     │
│ call receiveRecover(SnapshotOffer)          │
│ state = recovering                          │
└──────────┬──────────────────────────────────┘
           │ ReplayMessages(fromSeq)
           ▼
┌──────────────────────────┐
│ Journal plugin           │
│ (event stream)           │
└──────────┬───────────────┘
           │ ReplayedMessage(event1)
           │ ReplayedMessage(event2)
           │ ...
           │ RecoverySuccess
           ▼
┌─────────────────────────────────────────────┐
│ Eventsourced                                 │
│ call receiveRecover(event payloads)         │
│ call receiveRecover(RecoveryCompleted)      │
│ state = processingCommands                  │
│ unstash all messages                        │
└─────────────────────────────────────────────┘


 */

  // -------- Commands
  sealed trait Command

  private final case class ReceivedMessage(contents: String) extends Command

  private final case class SentMessage(contents: String) extends Command

  case object Print extends Command

  // -------- Events
  sealed trait Event

  private final case class MessageReceived(id: Int, contents: String) extends Event

  private final case class MessageSent(id: Int, contents: String) extends Event

  // -------- State (immutable)
  final case class State(
                          nextId: Int,
                          lastMessages: Vector[(String, String)],
                          sinceLastSnapshot: Int
                        )

  object State {
    val empty = State(0, Vector.empty, 0)
  }

  //Actor
   class ChatActor(owner: String, contact: String)
    extends PersistentActor
      with ActorLogging {

    override def persistenceId: String =
      s"$owner-$contact-chat"

    private var state: State = State.empty
    private val MaxMessages = 10

    private def applyEvent(event: Event): Unit = {
      state = event match {
        case MessageReceived(id, msg) =>
          updateState(contact, msg, id)

        case MessageSent(id, msg) =>
          updateState(owner, msg, id)
      }
    }

    private def updateState(sender: String, msg: String, id: Int): State = {
      //appending the message to Vector
      // constantly update to pick latest/last 10 messages
      //so when persisted only last latest 10 messages saved
      val updated =
        (state.lastMessages :+ (sender -> msg)).takeRight(MaxMessages)

      state
        .copy(
          nextId = id + 1,
          lastMessages = updated,
          sinceLastSnapshot = state.sinceLastSnapshot + 1
        )
    }

    override def receiveCommand: Receive = {
      case ReceivedMessage(msg) =>
        log.info("[ChatActor]:-> Received the ReceivedMessage Command ")
        persist(MessageReceived(state.nextId, msg)) {
          log.info("updating the state ")
          e =>
          applyEvent(e)
          maybeSnapshot()
        }

      case SentMessage(msg) =>
        log.info("[ChatActor]:-> Received the SentMessage Command ")
        persist(MessageSent(state.nextId, msg)) {
          log.info("updating the state ")
          e =>
          applyEvent(e)
          maybeSnapshot()
        }

      case Print =>
        log.info("[ChatActor]:-> Received the Print Command ")
        log.info(s"Messages: ${state.lastMessages}")

      case SaveSnapshotSuccess(_) =>
        log.info("Snapshot saved successfully")

      case SaveSnapshotFailure(_, ex) =>
        log.error(s"saving Snapshot failed: $ex")
    }

    override def receiveRecover: Receive = {
      case SnapshotOffer(_, snapshot: State) =>
        log.info("[Chat-Actor]:-> State recovering using Snapshot")
        state = snapshot

      case event: Event =>
        applyEvent(event)
    }

    private def maybeSnapshot(): Unit =
      if (state.sinceLastSnapshot >= MaxMessages) {
        log.info("CheckPointing-triggered-For the Actor State")
        saveSnapshot(state)
        state = state.copy(sinceLastSnapshot = 0)
      }
  }

  val system = ActorSystem("Snapshot")
   val chat = system.actorOf(Props(new ChatActor("daniel", "martin")))

  //  chat ! ReceivedMessage("Hi")
  //  chat ! SentMessage("Hello")
  //  chat ! Print



//  for (i <- 1 to 100) {
//    chat ! ReceivedMessage(s"Akka Rocks $i")
//    chat ! SentMessage(s"Akka Rules $i")
//  }

  // snapshots come in.
  chat ! Print

  /*
    event 1
    event 2
    event 3
    snapshot 1
    event 4
    snapshot 2
    event 5
    event 6
   */

  /*
    pattern:
    - after each persist, maybe save a snapshot (logic is up to you)
    - if you save a snapshot, handle the SnapshotOffer message in receiveRecover
    - (optional, but best practice) handle SaveSnapshotSuccess and SaveSnapshotFailure in receiveCommand
    - profit from the extra speed!!!
   */


}
