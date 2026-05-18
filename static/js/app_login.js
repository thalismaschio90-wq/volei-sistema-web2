document.addEventListener("DOMContentLoaded", () => {
    const splash = document.getElementById("splashApp");
    const loginApp = document.getElementById("loginApp");
    const loginInput = document.getElementById("login");
    const senhaInput = document.getElementById("senha");
    const lembrar = document.getElementById("lembrar-dados");
    const toggleSenha = document.getElementById("toggleSenha");
    const iconeOlho = document.getElementById("iconeOlho");

    const mostrarLogin = () => {
        if (splash) splash.classList.add("splash-hide");
        if (loginApp) loginApp.classList.add("login-show");
    };

    window.setTimeout(mostrarLogin, 1650);

    const loginSalvo = localStorage.getItem("vt_login");
    const senhaSalva = localStorage.getItem("vt_senha");

    if (loginInput && senhaInput && lembrar && loginSalvo && senhaSalva) {
        loginInput.value = loginSalvo;
        senhaInput.value = senhaSalva;
        lembrar.checked = true;
    }

    if (toggleSenha && senhaInput && iconeOlho) {
        toggleSenha.addEventListener("click", () => {
            const mostrando = senhaInput.type === "text";
            senhaInput.type = mostrando ? "password" : "text";

            iconeOlho.innerHTML = mostrando
                ? `<path d="M1 12s4-7 11-7 11 7 11 7-4 7-11 7S1 12 1 12z"/><circle cx="12" cy="12" r="3"/>`
                : `<path d="M17.94 17.94A10.94 10.94 0 0 1 12 19C5 19 1 12 1 12a20.29 20.29 0 0 1 5.06-5.94"/><path d="M9.9 4.24A10.73 10.73 0 0 1 12 4c7 0 11 8 11 8a20.8 20.8 0 0 1-3.17 4.36"/><path d="M14.12 14.12A3 3 0 0 1 9.88 9.88"/><line x1="1" y1="1" x2="23" y2="23"/>`;
        });
    }

    const form = document.querySelector(".form-app");
    if (form && loginInput && senhaInput && lembrar) {
        form.addEventListener("submit", () => {
            if (lembrar.checked) {
                localStorage.setItem("vt_login", loginInput.value);
                localStorage.setItem("vt_senha", senhaInput.value);
            } else {
                localStorage.removeItem("vt_login");
                localStorage.removeItem("vt_senha");
            }
        });
    }
});
