package mesosphere.marathon
package core.launchqueue.impl

import com.typesafe.scalalogging.StrictLogging
import mesosphere.marathon.core.instance.update.InstancesSnapshot
import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.core.launchqueue.impl.ReviveOffersState.Role
import mesosphere.marathon.state.RunSpecConfigRef

import mesosphere.marathon.Protos.Constraint
import mesosphere.marathon.Protos.Constraint.Operator
import mesosphere.mesos.Constraints

import org.apache.mesos.scheduler.Protos.{OfferConstraints => MesosOfferConstraints}
import org.apache.mesos.scheduler.Protos.{AttributeConstraint => MesosAttributeConstraint}
import org.apache.mesos.scheduler.Protos.AttributeConstraint.Selector.PseudoattributeType

object OfferConstraints {
  // Intermediate representation of a Mesos attribute constraint predicate.
  sealed trait Predicate;

  case class Exists() extends Predicate
  case class NotExists() extends Predicate
  case class TextEquals(value: String) extends Predicate
  case class TextNotEquals(value: String) extends Predicate
  case class TextMatches(regex: String) extends Predicate
  case class TextNotMatches(regex: String) extends Predicate

  private def buildPredicateProto(predicate: Predicate, builder: MesosAttributeConstraint.Predicate.Builder): Unit = {
    predicate match {
      case Exists() => builder.getExistsBuilder();
      case NotExists() => builder.getNotExistsBuilder();
      case TextEquals(value) => builder.getTextEqualsBuilder.setValue(value);
      case TextNotEquals(value) => builder.getTextNotEqualsBuilder.setValue(value);
      case TextMatches(regex) => builder.getTextMatchesBuilder.setRegex(regex);
      case TextNotMatches(regex) => builder.getTextNotMatchesBuilder.setRegex(regex);
    }
  }

  // TODO (asekretenko): move into Constraints
  private def buildSelectorProto(field: String, builder: MesosAttributeConstraint.Selector.Builder): Unit = {

    field match {
      case "hostname" | "@hostname" => builder.setPseudoattributeType(PseudoattributeType.HOSTNAME)
      case "@region" => builder.setPseudoattributeType(PseudoattributeType.REGION)
      case "@zone" => builder.setPseudoattributeType(PseudoattributeType.ZONE)
      case _ => builder.setAttributeName(field)
    }
  }

  // Intermediate representation of a Mesos attribute constraint.
  case class AttributeConstraint(field: String, predicate: Predicate) {
    def buildProto(builder: MesosAttributeConstraint.Builder): Unit = {
      buildSelectorProto(field, builder.getSelectorBuilder());
      buildPredicateProto(predicate, builder.getPredicateBuilder());
    }
  }

  private def getAgnosticConstraint(constraint: Constraint): AttributeConstraint = {
    val predicate: Predicate = constraint.getOperator match {
      case Operator.LIKE => TextMatches(constraint.getValue)
      case Operator.UNLIKE => TextNotMatches(constraint.getValue)
      case Operator.UNIQUE => Exists()
      case Operator.GROUP_BY => Exists()
      case Operator.MAX_PER => Exists()
      case Operator.CLUSTER => Exists()
      case Operator.IS => TextEquals(constraint.getValue)
    }

    AttributeConstraint(constraint.getField, predicate)
  }

  /*
   * An attribute constraint that is brought to life by existence of
   * an active instance under app's constraint, and is applied only as soon as
   * the app has enough active instances inducing this constraint.
   * This is used to translate GROUP_BY/UNIQUE/MAX_PER into offer constraints.
   *
   * Example: consider an app with a constraint "foo:MAX_PER:2".
   * After the first launch on an agent with an attribute "foo:bar",
   * the app has a single instance that set
   * `Induced(AttributeConstraint("foo", TextNotEquals("bar")), 2)`
   * which is not applied.
   * After the second launch on an agent with an attribute "foo:bar",
   * the app has two such instances, hence
   * `AttributeConstraint("foo", TextNotEquals("bar"))`
   * is applied from then onwards.
   */
  case class Induced(attributeConstraint: AttributeConstraint, minInstanceCountToApply: Int)

  /*
   * Returns offer constraints that should be set for further offers
   * due to already existing Instance for the RunSpec in question.
   */
  private def getInducedConstraint(constraint: Constraint, placed: Instance): Option[Induced] = {

    def makeInduced(predicate: Predicate, minInstanceCountToApply: Int): Some[Induced] = {
      Some(Induced(AttributeConstraint(constraint.getField, predicate), minInstanceCountToApply))
    }

    val placedValueReader = Constraints.readerForField(constraint.getField)._2

    placedValueReader(placed) match {
      case Some(value) =>
        constraint.getOperator match {
          case Operator.IS => None
          case Operator.LIKE => None
          case Operator.UNLIKE => None
          case Operator.CLUSTER => makeInduced(TextEquals(value), 1)
          case Operator.UNIQUE => makeInduced(TextNotEquals(value), 1)
          case Operator.MAX_PER => makeInduced(TextNotEquals(value), constraint.getValue.toInt)

          // TODO: Support induced constraints for GROUP_BY
          case Operator.GROUP_BY => None

        }
      // This covers the case of a scheduled instance
      case None => None
    }
  }

  private def getInducedByInstance(instance: Instance): Set[Induced] = {
    if (!instance.isActive) Set.empty
    else
      instance.runSpec.constraints
        .map(getInducedConstraint(_, instance))
        .flatMap(identity)
        .to(Set)
  }

  case class Group(constraints: Set[AttributeConstraint]) {
    def buildProto(builder: MesosOfferConstraints.RoleConstraints.Group.Builder): Unit = {
      constraints.foreach { _.buildProto(builder.addAttributeConstraintsBuilder()) }
    }
  }

  object Group { val empty = Group(Set.empty) }

  case class AppState(
      role: Role,
      constraints: Set[Constraint],
      induced: Map[Instance.Id, Set[Induced]],
      scheduledInstances: Set[Instance.Id],
      instancesToUnreserve: Set[Instance.Id]
  ) {

    def isEmpty(): Boolean = { induced.isEmpty && scheduledInstances.isEmpty && instancesToUnreserve.isEmpty }

    def willChangeOnUpdate(instance: Instance): Boolean = {
      val id = instance.instanceId

      role != instance.runSpec.role ||
      constraints != instance.runSpec.constraints ||
      induced.getOrElse(id, Set.empty) != getInducedByInstance(instance) ||
      scheduledInstances.contains(id) != instance.isScheduled ||
      instancesToUnreserve(id) != ReviveOffersState.shouldUnreserve(instance)
    }

    def withInstanceAddedOrUpdated(instance: Instance): AppState = {
      val id = instance.instanceId
      val inducedByInstance = getInducedByInstance(instance)

      copy(
        role = instance.runSpec.role,
        constraints = instance.runSpec.constraints,
        induced = if (inducedByInstance.isEmpty) induced - id else induced + (id -> inducedByInstance),
        scheduledInstances = if (instance.isScheduled) scheduledInstances + id else scheduledInstances - id,
        instancesToUnreserve = if (ReviveOffersState.shouldUnreserve(instance)) instancesToUnreserve + id else instancesToUnreserve - id
      )
    }

    def withInstanceDeleted(instance: Instance): AppState = {
      val newInduced = induced - instance.instanceId
      val newScheduledInstances = scheduledInstances - instance.instanceId
      val newInstancesToUnreserve = instancesToUnreserve - instance.instanceId
      if (newInduced.isEmpty && newScheduledInstances.isEmpty && newInstancesToUnreserve.isEmpty)
        AppState.empty
      else copy(role, constraints, newInduced, newScheduledInstances, newInstancesToUnreserve)
    }

    def toGroup(): Option[Group] = {
      if (!instancesToUnreserve.isEmpty) Some(Group.empty)
      else if (scheduledInstances.isEmpty) None
      else {
        val allInducedConstraints = induced.values.flatten
        val effectiveInducedConstraints = allInducedConstraints
          .groupBy(identity)
          .valuesIterator
          .map { (induced: Iterable[Induced]) =>
            {
              if (induced.size >= induced.head.minInstanceCountToApply)
                Some(induced.head.attributeConstraint)
              else None
            }
          }
          .flatten

        Some(
          Group(
            (effectiveInducedConstraints ++
              constraints.map(getAgnosticConstraint(_))).toSet
          )
        )
      }
    }
  }

  object AppState {
    val empty = AppState("", Set.empty, Map.empty, Set.empty, Set.empty)

    def fromSnapshotInstances(instances: Iterable[Instance]): AppState = {
      AppState(
        role = instances.head.runSpec.role,
        constraints = instances.head.runSpec.constraints,
        induced = instances
          .flatMap(instance => {
            val inducedByInstance = getInducedByInstance(instance)
            if (inducedByInstance.isEmpty) None else Some(instance.instanceId -> inducedByInstance)
          })
          .to(Map),
        scheduledInstances = instances
          .flatMap(instance => {
            if (instance.isScheduled) Some(instance.instanceId) else None
          })
          .to(Set),
        instancesToUnreserve = instances
          .flatMap(instance => {
            if (ReviveOffersState.shouldUnreserve(instance)) Some(instance.instanceId) else None
          })
          .to(Set)
      )
    }
  }

  case class RoleState(groupsByRole: Map[Role, Set[Group]]) {

    lazy val protobuf = {
      val builder = MesosOfferConstraints.newBuilder();

      groupsByRole.foreach {
        case (role, groups) => {
          val roleConstraintsBuilder =
            MesosOfferConstraints.RoleConstraints.newBuilder()

          groups.foreach { _.buildProto(roleConstraintsBuilder.addGroupsBuilder()) }
          builder.putRoleConstraints(role, roleConstraintsBuilder.build())
        }
      }

      builder.build()
    }
  }

  object RoleState { val empty = RoleState(Map.empty) }

  case class State(state: Map[RunSpecConfigRef, AppState]) extends StrictLogging {

    def withSnapshot(snapshot: InstancesSnapshot): State = {
      State(
        snapshot.instances
          .groupBy(_.runSpec.configRef)
          .view
          .mapValues(AppState.fromSnapshotInstances(_))
          .toMap
      )
    }

    def withInstanceAddedOrUpdated(instance: Instance): State = {
      val configRef = instance.runSpec.configRef
      val appState: AppState = state.getOrElse(configRef, AppState.empty)

      if (appState.willChangeOnUpdate(instance)) {
        val updated = copy(
          state +
            (configRef -> appState.withInstanceAddedOrUpdated(instance))
        )

        logger.info(s"Instance ${instance.instanceId} updates OfferConstraintsState to ${updated}")
        updated
      } else {
        this
      }
    }

    def withInstanceDeleted(instance: Instance): State = {
      val configRef = instance.runSpec.configRef
      val newAppState = state
        .getOrElse(configRef, AppState.empty)
        .withInstanceDeleted(instance)

      copy(
        if (newAppState.isEmpty())
          state - configRef
        else state + (configRef -> newAppState)
      )
    }

    lazy val roleState = RoleState({
      val appStatesByRole = state.values.groupBy(_.role)

      appStatesByRole.flatMap {
        case (role: Role, appStates: Iterable[AppState]) => {
          val groups: Set[Group] = appStates.flatMap(_.toGroup).toSet
          val roleShouldBeSuppressed = groups.isEmpty
          val roleNeedsUnconstrainedOffers = groups.contains(Group.empty)

          // NOTE: Decision to suppress a role is made independently by the ReviveOffersState.
          if (roleShouldBeSuppressed || roleNeedsUnconstrainedOffers)
            None
          else Some(role -> groups)
        }
      }
    })
  }

  object State { val empty = State(Map.empty) }
}
