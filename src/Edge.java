
public class Edge {
	private int a;
	private int b;
	public int getA() {
		return a;
	}
	public void setA(int a) {
		this.a = a;
	}
	public int getB() {
		return b;
	}
	public void setB(int b) {
		this.b = b;
	}
	public Edge(int a, int b) {
		super();
		this.a = a;
		this.b = b;
	}
	@Override
	public String toString() {
		return this.a + " " + this.b;
	}
	
	@Override
	public boolean equals(Object obj) {
		if(! (obj instanceof Edge)) {
			return false;
		}
		Edge that = (Edge) obj;
		return this.a == that.getA() && this.b == that.getB();
	}
	
	
}
