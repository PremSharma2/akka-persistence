package eventSourcing




  import akka.actor.{ActorLogging, ActorRef, ActorSystem, Props}
  import akka.pattern.{Backoff, BackoffSupervisor}
  import akka.persistence._
  import scala.concurrent.duration._

  object SnapShotExercise extends App {

    // ---------------- Commands / Replies ----------------

    sealed trait Command
    final case class Credit(commandId: String, amount: BigDecimal, currency: String, replyTo: ActorRef) extends Command
    final case class Debit(commandId: String, amount: BigDecimal, currency: String, replyTo: ActorRef) extends Command
    final case class GetBalance(replyTo: ActorRef) extends Command
    case object PrintState extends Command
    case object Shutdown extends Command

    //Response Commands sent by Actor to Another Actor
    sealed trait Reply
    final case class Accepted(balance: BigDecimal, currency: String) extends Reply
    final case class Rejected(reason: String) extends Reply
    final case class Balance(balance: BigDecimal, currency: String) extends Reply

    // ---------------- Events ----------------

    sealed trait Event { def commandId: String }
    final case class Credited(commandId: String, amount: BigDecimal, currency: String) extends Event
    final case class Debited(commandId: String, amount: BigDecimal, currency: String) extends Event

    // ---------------- State (snapshot payload) ----------------

    final case class State(
                            balance: BigDecimal,
                            currency: String,
                            recentCommandIds: Vector[String], // bounded idempotency window
                            eventsSinceSnapshot: Int
                          ) {
      def seen(id: String): Boolean = recentCommandIds.contains(id)
      def remember(id: String, max: Int): State =
        copy(recentCommandIds = (recentCommandIds :+ id).takeRight(max))
    }

    object State {
      val empty: State = State(BigDecimal(0), "GBP", Vector.empty, 0)
    }

    // ---------------- Persistent Actor ----------------

    final class LedgerActor(account: String, bic: String) extends PersistentActor with ActorLogging {

      override def persistenceId: String = s"ledger-$account-$bic"

      private var state: State = State.empty

      private val MaxRecentIds         = 200
      private val SnapshotEveryEvents  = 200

      // single source of truth for state evolution
      private def applyEvent(e: Event): Unit = {
        state = e match {
          case Credited(cmdId, amount, ccy) =>
            val newCcy = if (state.balance == 0) ccy else state.currency
            state.copy(
              balance = state.balance + amount,
              currency = newCcy,
              eventsSinceSnapshot = state.eventsSinceSnapshot + 1
            ).remember(cmdId, MaxRecentIds)

          case Debited(cmdId, amount, ccy) =>
            val newCcy = if (state.balance == 0) ccy else state.currency
            state.copy(
              balance = state.balance - amount,
              currency = newCcy,
              eventsSinceSnapshot = state.eventsSinceSnapshot + 1
            ).remember(cmdId, MaxRecentIds)
        }
      }

      private def maybeSnapshot(): Unit = {
        if (state.eventsSinceSnapshot >= SnapshotEveryEvents) {
          log.info(s"[ledger:$account:$bic] Saving snapshot at seqNr=$lastSequenceNr, balance=${state.balance}")
          saveSnapshot(state.copy(eventsSinceSnapshot = 0)) // snapshot payload should be immutable
          state = state.copy(eventsSinceSnapshot = 0)
        }
      }

      override def receiveCommand: Receive = {

        case Credit(cmdId, amount, ccy, replyTo) =>
          if (amount <= 0) replyTo ! Rejected(s"amount must be > 0, got $amount")
          else if (state.seen(cmdId)) replyTo ! Accepted(state.balance, state.currency) // idempotent retry
          else if (state.currency != ccy && state.balance != 0) replyTo ! Rejected(s"currency mismatch: state=${state.currency} cmd=$ccy")
          else {
            val event = Credited(cmdId, amount, ccy)
            persist(event) { persisted =>
              applyEvent(persisted)
              replyTo ! Accepted(state.balance, state.currency)
              maybeSnapshot()
            }
          }

        case Debit(cmdId, amount, ccy, replyTo) =>
          if (amount <= 0) replyTo ! Rejected(s"amount must be > 0, got $amount")
          else if (state.seen(cmdId)) replyTo ! Accepted(state.balance, state.currency) // idempotent retry
          else if (state.currency != ccy && state.balance != 0) replyTo ! Rejected(s"currency mismatch: state=${state.currency} cmd=$ccy")
          else if (state.balance < amount) replyTo ! Rejected(s"insufficient funds: balance=${state.balance} debit=$amount")
          else {
            val event = Debited(cmdId, amount, ccy)
            persist(event) { persisted =>
              applyEvent(persisted)
              replyTo ! Accepted(state.balance, state.currency)
              maybeSnapshot()
            }
          }

        case GetBalance(replyTo) =>
          replyTo ! Balance(state.balance, state.currency)

        case PrintState =>
          log.info(s"[ledger:$account:$bic] State=$state lastSeqNr=$lastSequenceNr")

        case SaveSnapshotSuccess(meta) =>
          log.info(s"[ledger:$account:$bic] Snapshot saved: $meta")

        case SaveSnapshotFailure(meta, ex) =>
          log.error(ex, s"[ledger:$account:$bic] Snapshot failed: $meta")

        case Shutdown =>
          context.stop(self)
      }

      // Recovery: snapshot first (if exists), then events after it
      override def receiveRecover: Receive = {

        case SnapshotOffer(meta, snapshot: State) =>
          log.info(s"[ledger:$account:$bic] SnapshotOffer seqNr=${meta.sequenceNr}, restoring snapshot")
          state = snapshot

        case e: Event =>
          applyEvent(e)

        case RecoveryCompleted =>
          log.info(s"[ledger:$account:$bic] RecoveryCompleted. balance=${state.balance}, lastSeqNr=$lastSequenceNr")
      }

      // persist failure => actor stops (supervisor restarts with backoff)
      override def onPersistFailure(cause: Throwable, event: Any, seqNr: Long): Unit = {
        log.error(cause, s"[ledger:$account:$bic] Persist FAILED seqNr=$seqNr event=$event -> stopping (supervisor will restart)")
        super.onPersistFailure(cause, event, seqNr)
      }
    }

    object LedgerActor {
      def props(account: String, bic: String): Props =
        Props(new LedgerActor(account, bic))
    }

    // ---------------- Bootstrapping with BackoffSupervisor ----------------

    val system = ActorSystem("BankLedgerClassic")

    val account = "GB29NWBK60161331926819"
    val bic     = "NWBKGB2L"

    val childProps = LedgerActor.props(account, bic)

    val supervisorProps =
      BackoffSupervisor.props(
        Backoff.onStop(
          childProps = childProps,
          childName = "ledger",
          minBackoff = 1.second,
          maxBackoff = 30.seconds,
          randomFactor = 0.2
        )
      )

    val ledger = system.actorOf(supervisorProps, "ledgerSupervisor")

    // In production, API layer would be the sender/replyTo
    val replyPrinter = system.actorOf(Props(new akka.actor.Actor with ActorLogging {
      def receive: Receive = { case r => log.info(s"[reply] $r") }
    }))

    ledger ! Credit("cmd-1", BigDecimal(1000), "GBP", replyPrinter)
    ledger ! Debit("cmd-2", BigDecimal(250), "GBP", replyPrinter)
    ledger ! GetBalance(replyPrinter)
    ledger ! PrintState


}
