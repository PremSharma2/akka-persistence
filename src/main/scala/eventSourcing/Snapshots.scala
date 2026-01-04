package eventSourcing

import akka.actor.{ActorLogging, ActorSystem, Props}
import akka.persistence.{PersistentActor, SaveSnapshotFailure, SaveSnapshotSuccess, SnapshotOffer}

object Snapshots extends App {


  // -------- Commands
  sealed trait Command

  final case class ReceivedMessage(contents: String) extends Command

  final case class SentMessage(contents: String) extends Command

  case object Print extends Command

  // -------- Events
  sealed trait Event

  final case class MessageReceived(id: Int, contents: String) extends Event

  final case class MessageSent(id: Int, contents: String) extends Event

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
  private class ChatActor(owner: String, contact: String)
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

      state.copy(
        nextId = id + 1,
        lastMessages = updated,
        sinceLastSnapshot = state.sinceLastSnapshot + 1
      )
    }

    override def receiveCommand: Receive = {
      case ReceivedMessage(msg) =>
        persist(MessageReceived(state.nextId, msg)) { e =>
          applyEvent(e)
          maybeSnapshot()
        }

      case SentMessage(msg) =>
        persist(MessageSent(state.nextId, msg)) { e =>
          applyEvent(e)
          maybeSnapshot()
        }

      case Print =>
        log.info(s"Messages: ${state.lastMessages}")

      case SaveSnapshotSuccess(_) =>
        log.info("Snapshot saved successfully")

      case SaveSnapshotFailure(_, ex) =>
        log.error(s"Snapshot failed: $ex")
    }

    override def receiveRecover: Receive = {
      case SnapshotOffer(_, snapshot: State) =>
        state = snapshot

      case event: Event =>
        applyEvent(event)
    }

    private def maybeSnapshot(): Unit =
      if (state.sinceLastSnapshot >= MaxMessages) {
        saveSnapshot(state)
        state = state.copy(sinceLastSnapshot = 0)
      }
  }

  val system = ActorSystem("SnapshotsRefactored")
  val chat = system.actorOf(Props(new ChatActor("daniel", "martin")))

//  chat ! ReceivedMessage("Hi")
//  chat ! SentMessage("Hello")
//  chat ! Print



    for (i <- 1 to 100000) {
      chat ! ReceivedMessage(s"Akka Rocks $i")
      chat ! SentMessage(s"Akka Rules $i")
    }

  // snapshots come in.
  chat ! "print"

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
