package bean

/**
 * @description: ConstraintStatus
 * @author: HanYunsong
 * @create: 2021-12-09 11:35
 */
sealed trait ConstraintStatus {
  val stringValue: String
}

object ConstraintSuccess extends ConstraintStatus {
  val stringValue = "Success"
}

object ConstraintFailure extends ConstraintStatus {
  val stringValue = "Failure"
}

case class ConstraintError(throwable: Throwable) extends ConstraintStatus {
  val stringValue = "Error"
}
