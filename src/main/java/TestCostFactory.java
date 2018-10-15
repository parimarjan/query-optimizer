import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptUtil;

import java.util.Objects;

/**
 * <code>TestCost</code> represents the cost of a plan node.
 *
 * <p>This class is immutable: none of the methods modify any member
 * variables.</p>
 */
class TestCost implements RelOptCost {
  //~ Static fields/initializers ---------------------------------------------

  static final TestCost INFINITY =
      new TestCost(
          Double.POSITIVE_INFINITY,
          Double.POSITIVE_INFINITY,
          Double.POSITIVE_INFINITY) {
        public String toString() {
          return "{inf}";
        }
      };

  static final TestCost HUGE =
      new TestCost(Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE) {
        public String toString() {
          return "{huge}";
        }
      };

  static final TestCost ZERO =
      new TestCost(0.0, 0.0, 0.0) {
        public String toString() {
          return "{0}";
        }
      };

  static final TestCost TINY =
      new TestCost(1.0, 1.0, 0.0) {
        public String toString() {
          return "{tiny}";
        }
      };

  public static final RelOptCostFactory FACTORY = new Factory();

  //~ Instance fields --------------------------------------------------------

  final double cpu;
  final double io;
  final double rowCount;

  //~ Constructors -----------------------------------------------------------

  TestCost(double rowCount, double cpu, double io) {
    this.rowCount = rowCount;
    this.cpu = cpu;
    this.io = io;
  }

  //~ Methods ----------------------------------------------------------------

  public double getCpu() {
    return cpu;
  }

  public boolean isInfinite() {
    return (this == INFINITY)
        || (this.rowCount == Double.POSITIVE_INFINITY)
        || (this.cpu == Double.POSITIVE_INFINITY)
        || (this.io == Double.POSITIVE_INFINITY);
  }

  public double getIo() {
    return io;
  }

  public boolean isLe(RelOptCost other) {
	return true;
    //TestCost that = (TestCost) other;
    //if (true) {
      //return this == that
          //|| this.rowCount <= that.rowCount;
    //}
    //return (this == that)
        //|| ((this.rowCount <= that.rowCount)
        //&& (this.cpu <= that.cpu)
        //&& (this.io <= that.io));
  }

  public boolean isLt(RelOptCost other) {
    if (true) {
      TestCost that = (TestCost) other;
      return this.rowCount < that.rowCount;
    }
    return isLe(other) && !equals(other);
  }

  public double getRows() {
    return rowCount;
  }

  @Override public int hashCode() {
    return Objects.hash(rowCount, cpu, io);
  }

  public boolean equals(RelOptCost other) {
    return this == other
        || other instanceof TestCost
        && (this.rowCount == ((TestCost) other).rowCount)
        && (this.cpu == ((TestCost) other).cpu)
        && (this.io == ((TestCost) other).io);
  }

  public boolean isEqWithEpsilon(RelOptCost other) {
    if (!(other instanceof TestCost)) {
      return false;
    }
    TestCost that = (TestCost) other;
    return (this == that)
        || ((Math.abs(this.rowCount - that.rowCount) < RelOptUtil.EPSILON)
        && (Math.abs(this.cpu - that.cpu) < RelOptUtil.EPSILON)
        && (Math.abs(this.io - that.io) < RelOptUtil.EPSILON));
  }

  public RelOptCost minus(RelOptCost other) {
    if (this == INFINITY) {
      return this;
    }
    TestCost that = (TestCost) other;
    return new TestCost(
        this.rowCount - that.rowCount,
        this.cpu - that.cpu,
        this.io - that.io);
  }

  public RelOptCost multiplyBy(double factor) {
    if (this == INFINITY) {
      return this;
    }
    return new TestCost(rowCount * factor, cpu * factor, io * factor);
  }

  public double divideBy(RelOptCost cost) {
    // Compute the geometric average of the ratios of all of the factors
    // which are non-zero and finite.
    TestCost that = (TestCost) cost;
    double d = 1;
    double n = 0;
    if ((this.rowCount != 0)
        && !Double.isInfinite(this.rowCount)
        && (that.rowCount != 0)
        && !Double.isInfinite(that.rowCount)) {
      d *= this.rowCount / that.rowCount;
      ++n;
    }
    if ((this.cpu != 0)
        && !Double.isInfinite(this.cpu)
        && (that.cpu != 0)
        && !Double.isInfinite(that.cpu)) {
      d *= this.cpu / that.cpu;
      ++n;
    }
    if ((this.io != 0)
        && !Double.isInfinite(this.io)
        && (that.io != 0)
        && !Double.isInfinite(that.io)) {
      d *= this.io / that.io;
      ++n;
    }
    if (n == 0) {
      return 1.0;
    }
    return Math.pow(d, 1 / n);
  }

  public RelOptCost plus(RelOptCost other) {
    TestCost that = (TestCost) other;
    if ((this == INFINITY) || (that == INFINITY)) {
      return INFINITY;
    }
    return new TestCost(
        this.rowCount + that.rowCount,
        this.cpu + that.cpu,
        this.io + that.io);
  }

  public String toString() {
    return "{" + rowCount + " rows, " + cpu + " cpu, " + io + " io}";
  }

  /** Implementation of {@link org.apache.calcite.plan.RelOptCostFactory}
   * that creates {@link org.apache.calcite.plan.volcano.TestCost}s. */
  private static class Factory implements RelOptCostFactory {
    public RelOptCost makeCost(double dRows, double dCpu, double dIo) {
	  return new TestCost(dRows, dCpu, dIo);
    }

    public RelOptCost makeHugeCost() {
      return TestCost.HUGE;
    }

    public RelOptCost makeInfiniteCost() {
      return TestCost.INFINITY;
    }

    public RelOptCost makeTinyCost() {
      return TestCost.TINY;
    }

    public RelOptCost makeZeroCost() {
      return TestCost.ZERO;
    }
  }
}

