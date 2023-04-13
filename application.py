from typing import Union

from fastapi import FastAPI
import dataEng

app = FastAPI()

@app.get("/api/v1/recomendation/{id}")
async def read_item(id: str):
    data = dataEng.getData(id)
    return {"id": id, "data": data, 'message': 'Haswin babi!'}