# main.py
from fastapi import FastAPI
import uvicorn
from master_git_vers.routers import builtins, transform


app = FastAPI()

app.include_router(builtins.router)
app.include_router(transform.router)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
