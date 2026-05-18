(function () {
    function initSplash() {
        const splash = document.querySelector('[data-app-splash]');
        if (!splash) return;

        const minimumTime = Number(splash.dataset.time || 1200);

        window.setTimeout(function () {
            splash.classList.add('hide');
            window.setTimeout(function () {
                splash.remove();
            }, 650);
        }, minimumTime);
    }

    function initPasswordToggle() {
        document.querySelectorAll('[data-toggle-password]').forEach(function (button) {
            const targetSelector = button.getAttribute('data-toggle-password');
            const input = document.querySelector(targetSelector);
            if (!input) return;

            button.addEventListener('click', function () {
                const showing = input.getAttribute('type') === 'text';
                input.setAttribute('type', showing ? 'password' : 'text');
                button.setAttribute('aria-label', showing ? 'Mostrar senha' : 'Ocultar senha');
                button.textContent = showing ? '👁' : '🙈';
            });
        });
    }

    function markStandaloneMode() {
        const isStandalone =
            window.matchMedia('(display-mode: standalone)').matches ||
            window.navigator.standalone === true;

        document.documentElement.classList.toggle('is-pwa', isStandalone);
    }

    document.addEventListener('DOMContentLoaded', function () {
        markStandaloneMode();
        initSplash();
        initPasswordToggle();
    });
})();
