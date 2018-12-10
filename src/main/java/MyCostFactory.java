import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptUtil;

import java.util.Objects;

/**
 * <code>MyCost</code> represents the cost of a plan node.
 *
 * <p>This class is immutable: none of the methods modify any member
 * variables.</p>
 */
class MyCost implements RelOptCost {
  //~ Static fields/initializers ---------------------------------------------

  static final MyCost INFINITY =
      new MyCost(
          Double.POSITIVE_INFINITY,
          Double.POSITIVE_INFINITY,
          Double.POSITIVE_INFINITY) {
        public String toString() {
          return "{inf}";
        }
      };

  static final MyCost HUGE =
      new MyCost(Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE) {
        public String toString() {
          return "{huge}";
        }
      };

  static final MyCost ZERO =
      new MyCost(0.0, 0.0, 0.0) {
        public String toString() {
          return "{0}";
        }
      };

  static final MyCost TINY =
      new MyCost(1.0, 1.0, 0.0) {
        public String toString() {
          return "{tiny}";
        }
      };

  public static final RelOptCostFactory FACTORY = new Factory();

  //~ Instance fields --------------------------------------------------------

  final double cpu;
  final double io;
  double rowCount;

  //~ Constructors -----------------------------------------------------------

  MyCost(double rowCount, double cpu, double io) {
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
		// FIXME:
		MyCost that = (MyCost) other;
		if (true) {
			return this == that
					//|| this.rowCount <= that.rowCount;
					|| this.getRealRowCount() <= that.getRealRowCount();
		}
		return (this == that)
				|| ((this.rowCount <= that.rowCount)
				&& (this.cpu <= that.cpu)
				&& (this.io <= that.io));
  }

  public boolean isLt(RelOptCost other) {
    if (true) {
      MyCost that = (MyCost) other;
      //return this.rowCount < that.rowCount;
      return this.getRealRowCount() < that.getRealRowCount();
    }
    return isLe(other) && !equals(other);
  }

  private double getRealRowCount() {
    int MEMORY_LIMIT = 1000;
    int OVERFLOW_PENALTY = MEMORY_LIMIT*2;

    int numOverflows = ((int) this.rowCount) / MEMORY_LIMIT;
    //return this.rowCount + numOverflows * OVERFLOW_PENALTY;
    return (this.rowCount / 1e8) + numOverflows * OVERFLOW_PENALTY;
  }

  public double getRows() {
    return rowCount;
  }

  @Override public int hashCode() {
    return Objects.hash(rowCount, cpu, io);
  }

  public boolean equals(RelOptCost other) {
    return this == other
        || other instanceof MyCost
        && (this.rowCount == ((MyCost) other).rowCount)
        && (this.cpu == ((MyCost) other).cpu)
        && (this.io == ((MyCost) other).io);
  }

  public boolean isEqWithEpsilon(RelOptCost other) {
    if (!(other instanceof MyCost)) {
      return false;
    }
    MyCost that = (MyCost) other;
    return (this == that)
        || ((Math.abs(this.getRealRowCount() - that.getRealRowCount()) < RelOptUtil.EPSILON)
        && (Math.abs(this.cpu - that.cpu) < RelOptUtil.EPSILON)
        && (Math.abs(this.io - that.io) < RelOptUtil.EPSILON));
  }

  public RelOptCost minus(RelOptCost other) {
    if (this == INFINITY) {
      return this;
    }
    MyCost that = (MyCost) other;
    return new MyCost(
        this.rowCount - that.rowCount,
        this.cpu - that.cpu,
        this.io - that.io);
  }

  public RelOptCost multiplyBy(double factor) {
    if (this == INFINITY) {
      return this;
    }
    return new MyCost(rowCount * factor, cpu * factor, io * factor);
  }

  public double divideBy(RelOptCost cost) {
    // Compute the geometric average of the ratios of all of the factors
    // which are non-zero and finite.
    MyCost that = (MyCost) cost;
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
    MyCost that = (MyCost) other;
    if ((this == INFINITY) || (that == INFINITY)) {
      return INFINITY;
    }
    return new MyCost(
        this.rowCount + that.rowCount,
        this.cpu + that.cpu,
        this.io + that.io);
  }

  public String toString() {
    return "{" + rowCount + " rows, " + cpu + " cpu, " + io + " io}";
  }

  /** Implementation of {@link org.apache.calcite.plan.RelOptCostFactory}
   * that creates {@link org.apache.calcite.plan.volcano.MyCost}s. */
  private static class Factory implements RelOptCostFactory {
    public RelOptCost makeCost(double dRows, double dCpu, double dIo) {
      return new MyCost(dRows, dCpu, dIo);
    }

    public RelOptCost makeHugeCost() {
      return MyCost.HUGE;
    }

    public RelOptCost makeInfiniteCost() {
      return MyCost.INFINITY;
    }

    public RelOptCost makeTinyCost() {
      return MyCost.TINY;
    }

    public RelOptCost makeZeroCost() {
      return MyCost.ZERO;
    }
  }
}
