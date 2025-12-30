package eventSourcing

package eventSourcing

import java.util.Date

import akka.actor.{ActorLogging, ActorRef, ActorSystem, Props}
import akka.pattern.{Backoff, BackoffSupervisor}
import akka.persistence.PersistentActor

import scala.concurrent.duration._

object PersistentActorsRefactored extends App {

  // -----------------------------
  // Protocol (commands + replies)
  // -----------------------------
  //Input Command
  sealed trait Command

  final case class Invoice(recipient: String, date: Date, amount: Int) extends Command

  private case object Shutdown extends Command

  private case object PrintState extends Command

  //Response Command
  private sealed trait Reply

  private case object PersistenceAck extends Reply

  private final case class Rejected(reason: String) extends Reply

  // -----------------------------
  // Events (persisted)
  // -----------------------------
  sealed trait Event

  final case class InvoiceRecorded(id: Int, recipient: String, date: Date, amount: Int) extends Event

  // -----------------------------
  // State (immutable)
  // -----------------------------
  final case class State(nextInvoiceId: Int, totalAmount: Long)

  private object State {
    val empty: State = State(nextInvoiceId = 0, totalAmount = 0L)
  }

  private class AccountantActor extends PersistentActor with ActorLogging {

    override val persistenceId: String = "simple-accountant" // in prod: include unique business/entity key

    private var state: State = State.empty

    // single source of truth: apply event => new state
    private def applyEvent(s: State, e: InvoiceRecorded): State =
      s.copy(
        nextInvoiceId = math.max(s.nextInvoiceId, e.id + 1), // avoids id reuse on recovery
        totalAmount = s.totalAmount + e.amount.toLong
      )

    override def receiveCommand: Receive = {

      case Invoice(recipient, date, amount) =>
        val replyTo: ActorRef = sender() // capture sender safely

        if (amount <= 0) {
          replyTo ! Rejected(s"amount must be > 0, got $amount")
        } else {

          val event = InvoiceRecorded(
            id = state.nextInvoiceId,
            recipient = recipient,
            date = date,
            amount = amount
          )

          log.info(s"[AccountantActor]:-> Received invoice amount=$amount -> persist invoiceId=${event.id}")

          // Persist first, then update state, then reply
          persist(event) { persisted =>
            state = applyEvent(state, persisted)
            replyTo ! PersistenceAck
            log.info(s"[AccountantActor]:-> Persisted invoiceId=${persisted.id}, totalAmount=${state.totalAmount}")
          }
        }

      case PrintState =>
        log.info(s"[AccountantActor] State: nextInvoiceId=${state.nextInvoiceId}, totalAmount=${state.totalAmount}")

      case Shutdown =>
        context.stop(self)
    }

    override def receiveRecover: Receive = {
      case e: InvoiceRecorded =>
        //replaying all events and building the state to the last available state when actor died
        state = applyEvent(state, e)
        log.info(s"[AccountantActor] Recovered invoiceId=${e.id}, totalAmount=${state.totalAmount}")
    }

    // Persist failure => actor will STOP (Akka Persistence behavior). We'll restart via supervisor.
    override def onPersistFailure(cause: Throwable, event: Any, seqNr: Long): Unit = {
      log.error(cause, s"[AccountantActor] Persist FAILED seqNr=$seqNr event=$event -> stopping (will be restarted by supervisor)")
      super.onPersistFailure(cause, event, seqNr)
    }

    // Persist rejected => actor resumes; decide how you want to react
    override def onPersistRejected(cause: Throwable, event: Any, seqNr: Long): Unit = {
      log.error(cause, s"[AccountantActor] Persist REJECTED seqNr=$seqNr event=$event -> actor resumes")
      super.onPersistRejected(cause, event, seqNr)
    }
  }

  // -----------------------------
  // Supervision: BackoffSupervisor
  // -----------------------------
  val system = ActorSystem("PersistentActors")

  private val childProps = Props(new AccountantActor)

  // Restarts the child after failures with exponential backoff.
  /**
   * BackoffSupervisor is a normal actor that Akka provides, whose behavior is:
   *
   * spawn a child
   *
   * watch the child
   *
   * if child stops, restart after backoff delay
   *
   * forward your incoming messages to the current child
   *
   * So yes: it’s “just an actor” with a specific built-in behavior.
   */
  private val supervisorProps =
    BackoffSupervisor
      .props(
        Backoff
          .onStop(
            childProps = childProps,
            childName = "simpleAccountant",
            minBackoff = 1.second,
            maxBackoff = 30.seconds,
            randomFactor = 0.2 // adds jitter
          )
      )

  val accountantSupervisor = system.actorOf(supervisorProps, "accountantSupervisor")

  // NOTE: You send messages to the supervisor; it forwards to the child.
  // If the child stops (e.g., persist failure), it will be restarted automatically.
  for (i <- 1 to 10) {
    accountantSupervisor ! Invoice("The Sofa Company", new Date, i * 1000)
  }

  accountantSupervisor ! PrintState
}
