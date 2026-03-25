from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from scraper.stocktitan import scrape  

app = FastAPI()

# Serve HTML + static files
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/", response_class=HTMLResponse)
def home():
    with open("template/index.html", "r", encoding="utf-8") as f:
        return f.read()

@app.post("/run")
async def run(request: Request):

    body = await request.json()
    limit = body.get("limit", 10)

    articles = scrape(limit)  # call your existing Python function
    return {"articles": articles}