function jsonToLines(data) {
    return data.articles.map(a =>
`Ticker: ${a.ticker}
Title: ${a.title}
URL: ${a.url}
Tags: ${a.tags.join(", ")}
Sentiment: ${a.sentiment}
Score: ${a.score.toFixed(2)}
-----------------------------`).join("\n");
}

async function run() {
    const limit = document.getElementById("limit").value;

    const res = await fetch("/run", {
        method: "POST",
        headers: {
            "Content-Type": "application/json"
        },
        body: JSON.stringify({ limit: parseInt(limit) })
    });

    const data = await res.json();
    document.getElementById("output").innerText = jsonToLines(data);
}