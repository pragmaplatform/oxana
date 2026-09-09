document.addEventListener("DOMContentLoaded", () => {
        for (const notice of document.querySelectorAll("[data-auto-dismiss-notice]")) {
            setTimeout(() => {
                notice.style.transition = "opacity 200ms ease, transform 200ms ease";
                notice.style.opacity = "0";
                notice.style.transform = "translateY(-4px)";
                setTimeout(() => notice.remove(), 220);
            }, 3000);
        }
    });
