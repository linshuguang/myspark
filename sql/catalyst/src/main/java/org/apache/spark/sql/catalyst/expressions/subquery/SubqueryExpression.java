package org.apache.spark.sql.catalyst.expressions.subquery;

import lombok.Data;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.namedExpressions.ExprId;
import org.apache.spark.sql.catalyst.parser.ParserUtils;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

import javax.swing.text.ParagraphView;
import java.util.List;

/**
 * Created by kenya on 2019/2/22.
 */
@Data
public abstract class SubqueryExpression extends  PlanExpression<LogicalPlan> {
    LogicalPlan plan;
    List<Expression> children;
    ExprId exprId;

    public SubqueryExpression(
            LogicalPlan plan,
            List<Expression> children,
            ExprId exprId){
        this.plan = plan;
        this.children = children;
        this.exprId = exprId;
    }

    @Override
    protected List<Expression> children() {
        return children;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof SubqueryExpression) {
            SubqueryExpression s = (SubqueryExpression) o;

            return ParserUtils.equals(plan, s.plan)
                    && ParserUtils.equalList(children, s.children)
                    && ParserUtils.equals(exprId, s.exprId);
        }
        return false;
    }

}
