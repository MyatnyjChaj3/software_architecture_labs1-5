import os
from datetime import datetime, timedelta, timezone

import httpx
import jwt
from fastapi import Depends, FastAPI, HTTPException, Request, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm

# --- JWT Configuration ---
SECRET_KEY = "key"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

app = FastAPI(title="API Gateway")

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

# --- Dummy User Database ---
DUMMY_USERS_DB = {
    "user1": {
        "username": "user1",
        "full_name": "User One",
        "email": "user1@example.com",
        "hashed_password": "$2b$12$EixZA9WP3FBCs5gZ7C.a9uPSDcXqPTij6l32XxtHEkps09aZgRO0i",  # "password"
        "disabled": False,
    }
}
# --- Helper Functions for JWT ---
def create_access_token(data: dict, expires_delta: timedelta | None = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

async def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        user = DUMMY_USERS_DB.get(username)
        if user is None:
            raise credentials_exception
        return user
    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has expired",
            headers={"WWW-Authenticate": "Bearer"},
        )
    except jwt.PyJWTError:
        raise credentials_exception

# --- Token Endpoint ---
@app.post("/token")
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    user = DUMMY_USERS_DB.get(form_data.username)
    if not user or not form_data.password == "password":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Неверное имя пользователя или пароль",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user["username"]}, expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}

# --- Services ---
LAB1_SERVICE_URL = "http://lab1:8000/visits"
LAB2_SERVICE_URL = "http://lab2:8000/course-requirements"
LAB3_SERVICE_URL = "http://lab3:8000/group"

@app.get("/lab1/visits")
async def get_attendance_report(
    term: str,
    start_date: str,
    end_date: str,
    current_user: dict = Depends(get_current_user)
):
    params = {"term": term, "start_date": start_date, "end_date": end_date}
    async with httpx.AsyncClient(timeout=60.0) as client:
        try:
            print(f"Forwarding request to {LAB1_SERVICE_URL} with params: {params}")
            response = await client.get(LAB1_SERVICE_URL, params=params)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as exc:
            print(f"HTTP error occurred: {exc.response.status_code} - {exc.response.text}")
            raise HTTPException(
                status_code=exc.response.status_code, detail=exc.response.json() if exc.response.content else "Error from lab1 service"
            )
        except httpx.RequestError as exc:
            print(f"Request error occurred: {exc}")
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"Error connecting to lab1 service: {exc}",
            )
@app.get("/lab2/course-requirements")
async def get_course_requirements(
    course_name: str,
    semester: int,
    year: int,
    current_user: dict = Depends(get_current_user)
):
    params = {"course_name": course_name, "semester": semester, "year": year}
    async with httpx.AsyncClient(timeout=60.0) as client:
        try:
            print(f"Forwarding request to {LAB2_SERVICE_URL} with params: {params}")
            response = await client.get(LAB2_SERVICE_URL, params=params)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as exc:
            print(f"HTTP error occurred: {exc.response.status_code} - {exc.response.text}")
            raise HTTPException(
                status_code=exc.response.status_code,
                detail=exc.response.json() if exc.response.content else "Error from lab2 service"
            )
        except httpx.RequestError as exc:
            print(f"Request error occurred: {exc}")
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"Error connecting to lab2 service: {exc}",
            )
        
@app.get("/lab3/group")
async def get_group_attendance(
    group_name: str,
    current_user: dict = Depends(get_current_user)
):
    params = {"group_name": group_name}
    async with httpx.AsyncClient(timeout=60.0) as client:
        try:
            print(f"Forwarding request to {LAB3_SERVICE_URL} with params: {params}")
            response = await client.get(LAB3_SERVICE_URL, params=params)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as exc:
            print(f"HTTP error occurred: {exc.response.status_code} - {exc.response.text}")
            raise HTTPException(
                status_code=exc.response.status_code, detail=exc.response.json() if exc.response.content else "Error from lab3 service"
            )
        except httpx.RequestError as exc:
            print(f"Request error occurred: {exc}")
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"Error connecting to lab3 service: {exc}",
            )
        
@app.get("/")
async def root():
    return {"message": "API Gateway is running."}
