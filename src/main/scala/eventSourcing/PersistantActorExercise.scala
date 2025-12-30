package eventSourcing

import akka.actor.Actor.Receive
import akka.actor.{ActorLogging, ActorSystem, Props}
import akka.persistence.PersistentActor

import scala.collection.mutable

object PersistantActorExercise extends App {
  /*
      Persistent actor for a voting station
      Keep:
        - the citizens who voted
        - the poll: mapping between a candidate and the number of received votes so far

      The actor must be able to recover its state if it's shut down or restarted
     */
  sealed trait InputCommand

  case class Vote(citizenPID: String, candidate: String) extends InputCommand

  case object Print extends InputCommand

  case class VoteRecorded(id: String, citizenPid: String, candidate: String)

  class VotingStationActor extends PersistentActor with ActorLogging {
    override def persistenceId: String = "simple-voting-station"

    // ignore the mutable state for now
    val citizens: mutable.Set[String] = new mutable.HashSet[String]()
    val poll: mutable.Map[String, Int] = new mutable.HashMap[String, Int]()

    override def receiveCommand: Receive = {

      case vote@Vote(citizenPID, candidate) =>

        /**
         * 1) create the event
         * 2) persist the event
         * 3) handle a state change after persisting is successful
         */

        if (!citizens.contains(vote.citizenPID)) {
          persist(vote) { _ => // COMMAND sourcing
            log.info(s"[VotingStationActor]:-> Persisted: $vote")
            handleInternalStateChange(citizenPID, candidate)
          }

        }
        else {
          log.warning(s"[VotingStationActor]:-> Citizen $citizenPID is trying to vote multiple times!")
        }
      case Print => log.info(s"Current state: \nCitizens: $citizens\n polls: $poll")

    }

    private def handleInternalStateChange(citizenPID: String, candidate: String) = {
      citizens.add(citizenPID)
      val votes = poll.getOrElse(candidate, 0)
      poll.put(candidate, votes + 1)
    }

    override def receiveRecover: Receive = {
      case vote@Vote(citizenPID, candidate) =>
        log.info(s"[VotingStationActor]:-> replay the Event in Recovery : $vote")
        handleInternalStateChange(citizenPID, candidate)
    }
  }

  val system = ActorSystem("PersistentActorsExercise")
  val votingStation = system.actorOf(Props[VotingStationActor], "simpleVotingStation")

  val votesMap = Map[String, String](
    "Alice" -> "Martin",
    "Bob" -> "Roland",
    "Charlie" -> "Martin",
    "David" -> "Jonas",
    "Daniel" -> "Martin"
  )
  votesMap.keys.foreach {
    citizen =>
      votingStation ! Vote(citizen, votesMap(citizen))
  }
  votingStation ! Print
}