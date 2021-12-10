package bean

/**
 * @description: ConstraintResult
 * @author: HanYunsong
 * @create: 2021-12-09 10:59
 */

case class ConstraintResult(constraintType: String,
                            ruleId: String,
                            entity: String,
                            actual: String,
                            status: String,
                            expectedValue: String
                           )
