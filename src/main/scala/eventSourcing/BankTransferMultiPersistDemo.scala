package eventSourcing



import akka.actor.{ActorLogging, ActorRef, ActorSystem, Props}
import akka.persistence.{PersistentActor, RecoveryCompleted}

import java.time.Instant
import java.util.UUID

object BankTransferMultiPersistDemo extends App {

  // ========= Commands =========
  sealed trait Command
  private final case class TransferFunds(
                                  fromAccount: String,
                                  toAccount: String,
                                  amount: BigDecimal,
                                  requestedBy: String
                                ) extends Command

  private final case class GetState(replyTo: ActorRef) extends Command

  // ========= Events =========
  sealed trait Event { def transferId: String; def ts: Instant }

  private final case class TransferInitiated(
                                      transferId: String,
                                      fromAccount: String,
                                      toAccount: String,
                                      amount: BigDecimal,
                                      requestedBy: String,
                                      ts: Instant = Instant.now()
                                    ) extends Event

  private final case class LedgerDebited(
                                  transferId: String,
                                  account: String,
                                  amount: BigDecimal,
                                  ts: Instant = Instant.now()
                                ) extends Event

  private final case class LedgerCredited(
                                   transferId: String,
                                   account: String,
                                   amount: BigDecimal,
                                   ts: Instant = Instant.now()
                                 ) extends Event

  private final case class ComplianceLogged(
                                     transferId: String,
                                     rule: String,
                                     outcome: String,
                                     ts: Instant = Instant.now()
                                   ) extends Event

  // Outbox event (replay-safe integration trigger)
  final case class OutboxMessageQueued(
                                        transferId: String,
                                        topic: String,
                                        payload: String,
                                        ts: Instant = Instant.now()
                                      ) extends Event

  // ========= State =========
  final case class State(
                          balances: Map[String, BigDecimal] = Map.empty.withDefaultValue(BigDecimal(0)),
                          transfers: Map[String, String] = Map.empty, // transferId -> status
                          outbox: Vector[OutboxMessageQueued] = Vector.empty
                        ) {
    def applyEvent(e: Event): State = e match {
      case TransferInitiated(id, _, _, _, _, _) =>
        copy(transfers = transfers.updated(id, "INITIATED"))

      case LedgerDebited(id, account, amt, _) =>
        copy(balances = balances.updated(account, balances(account) - amt))

      case LedgerCredited(id, account, amt, _) =>
        copy(balances = balances.updated(account, balances(account) + amt))

      case _: ComplianceLogged =>
        this // usually audit doesn't change core state

      case o: OutboxMessageQueued =>
        copy(outbox = outbox :+ o)
    }
  }

  // ========= External side-effect sinks =========
  // These are deliberately side effects (NOT persisted)
  private object ComplianceService {
    final case class Notify(transferId: String, message: String)
  }

  private class ComplianceService extends akka.actor.Actor with ActorLogging {
    override def receive: Receive = {
      case ComplianceService.Notify(id, msg) =>
        log.info(s"[COMPLIANCE] notify for transferId=$id msg=$msg")
    }
  }

  private object LedgerProjection {
    final case class Publish(event: Event)
  }

  private class LedgerProjection extends akka.actor.Actor with ActorLogging {
    override def receive: Receive = {
      case LedgerProjection.Publish(e) =>
        log.info(s"[LEDGER-PROJECTION] published: $e")
    }
  }

  // ========= Persistent Actor =========
  private class TransferAggregateActor(
                           complianceSink: ActorRef,
                           ledgerProjection: ActorRef
                         ) extends PersistentActor with ActorLogging {

    override def persistenceId: String = "transfer-aggregate-1"

    private var state: State = State(
      balances = Map(
        "A-100" -> BigDecimal(5000),
        "B-200" -> BigDecimal(1200)
      ).withDefaultValue(BigDecimal(0))
    )

    // ---- Pure decision: Command -> List[Event] ----
    private def decide(cmd: TransferFunds): Either[String, List[Event]] = {
      val TransferFunds(from, to, amount, requestedBy) = cmd

      if (amount <= 0) Left("Amount must be > 0")
      else if (from == to) Left("fromAccount and toAccount cannot be same")
      else if (state.balances(from) < amount) Left(s"Insufficient funds in $from")
      else {
        val transferId = UUID.randomUUID().toString
        val now = Instant.now()

        val complianceOutcome =
          if (amount >= 10000) ("HIGH_VALUE_TRANSFER", "REVIEW_REQUIRED")
          else ("STANDARD_CHECK", "PASS")

        val events = List[Event](
          TransferInitiated(transferId, from, to, amount, requestedBy, now),
          LedgerDebited(transferId, from, amount, now),
          LedgerCredited(transferId, to, amount, now),
          ComplianceLogged(transferId, complianceOutcome._1, complianceOutcome._2, now),
          OutboxMessageQueued(
            transferId,
            topic = "payments.transfer.completed",
            payload = s"""{"transferId":"$transferId","from":"$from","to":"$to","amount":$amount}""",
            ts = now
          )
        )

        Right(events)
      }
    }

    // ---- Side effects AFTER persistence ----
    private def afterPersistAll(events: List[Event]): Unit = {
      // Example: notify compliance + publish to projection
      events.collect { case c: ComplianceLogged => c }.foreach { c =>
        complianceSink ! ComplianceService.Notify(c.transferId, s"${c.rule} -> ${c.outcome}")
      }

      // projection can be at-least-once; it must be idempotent in real systems
      events.foreach(e => ledgerProjection ! LedgerProjection.Publish(e))

      // Outbox consumer would normally be separate; here just log it
      events.collect { case o: OutboxMessageQueued => o }.foreach { o =>
        log.info(s"[OUTBOX] queued topic=${o.topic} payload=${o.payload}")
      }
    }

    // ---- Command handling ----
    override def receiveCommand: Receive = {
      case cmd: TransferFunds =>
        decide(cmd) match {
          case Left(err) =>
            log.warning(s"[TransferAggregateActor]:-> Rejected transfer: $err, state=$state")

          case Right(events) =>
            // MULTI-PERSIST in order (atomic from the aggregate PoV)
            persistAll(events) { e =>
              state = state.applyEvent(e) // state changes from events ONLY
            }

            // This callback is invoked after all events are persisted & handlers run
            deferAsync(events) { _ =>
              afterPersistAll(events)
            }
        }

      case GetState(replyTo) =>
        replyTo ! state
    }

    // ---- Recovery ----
    override def receiveRecover: Receive = {
      case e: Event =>
        state = state.applyEvent(e)

      case RecoveryCompleted =>
        log.info(s"RecoveryCompleted. state=$state")
    }
  }

  // ========= Boot =========
  val system = ActorSystem("BankTransferMultiPersistSystem")

  val compliance = system.actorOf(Props[ComplianceService](), "compliance")
  val projection = system.actorOf(Props[LedgerProjection](), "ledgerProjection")

  val transferAgg = system.actorOf(Props(new TransferAggregateActor(compliance, projection)), "transferAgg")

  transferAgg ! TransferFunds("A-100", "B-200", BigDecimal(250), "prem")
  transferAgg ! TransferFunds("A-100", "B-200", BigDecimal(12000), "prem") // triggers review required
}
