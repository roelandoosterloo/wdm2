
public class Triangle {
	private Edge a;
	private Edge b;
	private Edge c;
	
	public Triangle(Edge a, Edge b, Edge c) {
		super();
		this.a = a;
		this.b = b;
		this.c = c;
	}
	public Triangle(){
		super();
	}
	public Edge getA() {
		return a;
	}
	public void setA(Edge a) {
		this.a = a;
	}
	public boolean canSetA(Edge a) {
		if(b == null) {
			if(c == null) {
				return true;
			}
			return c.getA() == a.getA() || c.getA() == a.getB() || c.getB() == a.getA() || c.getB() == a.getB();
				
		} else if(c == null) {			
			return b.getA() == a.getA() || b.getA() == a.getB() || b.getB() == a.getA() || b.getB() == a.getB();
		} else {
			int freeA = b.getA();
			int freeB = c.getA();
			if(b.getA() == c.getA() || b.getA() == c.getB()) {
				freeA = b.getB();
			}
			if(c.getA() == b.getA() || c.getA() == b.getB() ) {
				freeB = c.getB();
			}
			
			
			return (a.getA() == freeA && a.getB() == freeB) || (a.getB() == freeA && a.getA() == freeB);
		}
	}
	public boolean trySetA(Edge a) {
		if(this.canSetA(a)) {
			this.setA(a);
			return true;
		}
		return false;
	}
	
	public Edge getB() {
		return b;
	}
	public void setB(Edge b) {
		this.b = b;
	}
	public boolean canSetB(Edge b) {
		if(a == null) {
			if(c == null) {
				return true;
			}
			return c.getA() == b.getA() || c.getA() == b.getB() || c.getB() == b.getA() || c.getB() == b.getB();
				
		} else if(c == null) {
			return a.getA() == b.getA() || a.getA() == b.getB() || a.getB() == b.getA() || a.getB() == b.getB();
		} else {
			int freeA = a.getA();
			int freeB = c.getA();
			if(a.getA() == c.getA() || a.getA() == c.getB()) {
				freeA = a.getB();
			}
			if(c.getA() == a.getA() || c.getA() == a.getB() ) {
				freeB = c.getB();
			}
			
			return (b.getA() == freeA && b.getB() == freeB) || (b.getB() == freeA && b.getA() == freeB);
		}
	}
	public boolean trySetB(Edge b) {
		if(this.canSetB(b)) {
			this.setB(b);
			return true;
		}
		return false;
	}
	
	public Edge getC() {
		return c;
	}
	public void setC(Edge c) {
		this.c = c;
	}
	public boolean canSetC(Edge c) {
		if(a == null) {
			if(b == null) {
				return true;
			}
			return b.getA() == c.getA() || b.getA() == c.getB() || b.getB() == c.getA() || b.getB() == c.getB();
				
		} else if(b == null) {
			return a.getA() == c.getA() || a.getA() == c.getB() || a.getB() == c.getA() || a.getB() == c.getB();
		} else {
			int freeA = a.getA();
			int freeB = b.getA();
			if(a.getA() == b.getA() || a.getA() == b.getB()) {
				freeA = a.getB();
			}
			if(b.getA() == a.getA() || b.getA() == a.getB() ) {
				freeB = b.getB();
			}
			
			return (c.getA() == freeA && c.getB() == freeB) || (c.getB() == freeA && c.getA() == freeB);
		}
	}
	public boolean trySetC(Edge c) {
		if(this.canSetC(c)) {
			this.setC(c);
			return true;
		}
		return false;
	}
	
	public boolean isTriangle() {
		return a != null && b != null && c != null;
	}

}
