from fastapi import FastAPI, Request, Form, status
from fastapi.responses import RedirectResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.sessions import SessionMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.exceptions import HTTPException as StarletteHTTPException
from .config import settings

app = FastAPI(title="Worker Analytics")

# CORS (на будущее)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

WHITELIST = {"/login"}  # единственная публичная страница

class AuthRequiredMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        path = request.url.path.rstrip("/") or "/"
        user = request.scope.get("session", {}).get("user")  # безопасно читаем, даже если сессии еще нет

        # уже залогинен и лезет на /login -> на дашборд
        if path == "/login" and user:
            return RedirectResponse(url="/dashboard", status_code=status.HTTP_302_FOUND)
        # не залогинен и пытается на что угодно кроме /login -> на логин
        if not user and path not in WHITELIST:
            return RedirectResponse(url="/login", status_code=status.HTTP_302_FOUND)

        return await call_next(request)

# СНАЧАЛА навешиваем авторизационную мидлварь...
app.add_middleware(AuthRequiredMiddleware)
# ...А ПОТОМ SessionMiddleware (последняя добавленная выполняется ПЕРВОЙ)
app.add_middleware(SessionMiddleware, secret_key=settings.SECRET_KEY)

@app.get("/login", response_class=HTMLResponse)
def login_form():
    return """
    <h2>Вход</h2>
    <form method="post" action="/login">
      <input type="text" name="username" placeholder="Логин"/><br/>
      <input type="password" name="password" placeholder="Пароль"/><br/>
      <button type="submit">Войти</button>
    </form>
    """

@app.post("/login")
def login(username: str = Form(...), password: str = Form(...), request: Request = None):
    if username == settings.ADMIN_USERNAME and password == settings.ADMIN_PASSWORD:
        # теперь session точно инициализирована SessionMiddleware
        request.session["user"] = username
        return RedirectResponse(url="/dashboard", status_code=302)
    return HTMLResponse("<p>Неверный логин или пароль</p><a href='/login'>Попробовать снова</a>", status_code=401)

@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request):
    return "<h1>Добро пожаловать в дашборд Worker Analytics!</h1>"

@app.get("/")
def root(request: Request):
    if request.session.get("user"):
        return RedirectResponse(url="/dashboard", status_code=302)
    return RedirectResponse(url="/login", status_code=302)

@app.get("/logout")
def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/login", status_code=302)

@app.exception_handler(StarletteHTTPException)
def not_found_handler(request: Request, exc: StarletteHTTPException):
    if exc.status_code == 404:
        if request.session.get("user"):
            return RedirectResponse(url="/dashboard", status_code=302)
        return RedirectResponse(url="/login", status_code=302)
    return HTMLResponse(str(exc.detail), status_code=exc.status_code)
