import os
import sqlite3
import matplotlib.pyplot as plt
from dotenv import load_dotenv
from fastmcp import FastMCP
from fpdf import FPDF
from google import genai 

# 1. Configuration & Robust Pathing
load_dotenv()

# Get the absolute path to the directory where server.py lives
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Build absolute paths for the database and data folders
DB_FILE = os.path.join(BASE_DIR, "data", "processed", "supply_chain.db")
# Required for Cloud Success
PROCESSED_DATA_DIR = "/tmp"

# Ensure the processed data directory exists (required for cloud saving)
os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)

# Initialize the new Google GenAI Client
client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

# Initialize MCP Server
mcp = FastMCP("SupplyChainAuditor")

# 2. Tool: Data Auditing
@mcp.tool()
def audit_region_risk(region: str) -> str:
    """Calculates risk metrics for a specific supply chain region."""
    conn = sqlite3.connect(DB_FILE)
    query = """
    SELECT 
        COUNT(*) as total_orders,
        AVG(lead_time_variance) as avg_delay,
        SUM(CASE WHEN late_delivery_risk = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as risk_percent
    FROM orders WHERE order_region = ?
    """
    cursor = conn.cursor()
    cursor.execute(query, (region,))
    row = cursor.fetchone()
    conn.close()

    if not row or row[0] == 0:
        return f"No data found for region: {region}"

    return (f"Audit for {region}: {row[0]} orders analyzed. "
            f"Avg Delay: {row[1]:.2f} days. "
            f"Late Delivery Risk: {row[2]:.1f}%.")

# 3. Tool: Visual Analytics
@mcp.tool()
def generate_risk_chart(region: str) -> str:
    """Creates a bar chart showing delivery trends for a region."""
    conn = sqlite3.connect(DB_FILE)
    query = """
    SELECT category_name, AVG(lead_time_variance) as delay 
    FROM orders WHERE order_region = ? 
    GROUP BY category_name ORDER BY delay DESC LIMIT 5
    """
    cursor = conn.cursor()
    cursor.execute(query, (region,))
    df_chart = cursor.fetchall()
    conn.close()

    if not df_chart:
        return f"Could not find enough category data for {region}."

    categories = [row[0] for row in df_chart]
    delays = [row[1] for row in df_chart]

    plt.figure(figsize=(8, 5))
    plt.bar(categories, delays, color='skyblue')
    plt.title(f"Top Delay Categories in {region}")
    plt.ylabel("Avg Delay (Days)")
    plt.xticks(rotation=45)
    
    # Save using absolute path
    chart_path = os.path.join(PROCESSED_DATA_DIR, f"{region}_risk_chart.png")
    plt.savefig(chart_path, bbox_inches='tight')
    plt.close() 
    
    return f" Chart generated and saved to {chart_path}"

# 4. Tool: Professional Reporting
@mcp.tool()
def export_audit_pdf(region: str, audit_summary: str) -> str:
    """Generates a professional PDF audit report using the new Gemini SDK."""
    prompt = f"Act as a Supply Chain Expert. Write a formal 2-paragraph risk mitigation memo for {region} based on: {audit_summary}."
    
    response = client.models.generate_content(
        model="gemini-2.0-flash", 
        contents=prompt
    )
    narrative = response.text

    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", 'B', 16)
    pdf.cell(0, 10, f"SUPPLY CHAIN RISK AUDIT: {region}", ln=True, align='C')
    
    pdf.set_font("Arial", size=12)
    pdf.ln(10)
    pdf.multi_cell(0, 10, narrative)
    
    # Load chart using absolute path
    chart_path = os.path.join(PROCESSED_DATA_DIR, f"{region}_risk_chart.png")
    if os.path.exists(chart_path):
        pdf.image(chart_path, x=10, y=pdf.get_y() + 10, w=180)

    pdf_filename = f"{region.replace(' ', '_')}_Audit_Report.pdf"
    pdf_path = os.path.join(PROCESSED_DATA_DIR, pdf_filename)
    pdf.output(pdf_path)
    return f" Professional Report exported to {pdf_path}"

# 5.
@mcp.tool()
def list_available_regions() -> str:
    """Returns a unique list of all regions present in the supply chain database."""
    conn = sqlite3.connect(DB_FILE)
    query = "SELECT DISTINCT order_region FROM orders ORDER BY order_region ASC"
    regions = [row[0] for row in conn.execute(query).fetchall()]
    conn.close()
    return "Available regions: " + ", ".join(regions)

if __name__ == "__main__":
    # Local Test: python server.py (Listens on port 8000)
    # Cloud: Entrypoint remains 'server.py'
    mcp.run(transport="http", port=8000)