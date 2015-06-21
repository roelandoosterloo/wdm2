
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
		return "Edge: " + this.a + " " + this.b;
	}
	
	@Override
	public boolean equals(Object other) {
		if( other instanceof Edge) {
			Edge that = (Edge)other;
			return that.getA() == this.getA() && that.getB() == this.getB();
		}
		return false;
	}
	
	
}
