"use strict";

document.addEventListener("DOMContentLoaded", () => {
    document.querySelectorAll(".entry.has-children > p").forEach(entry => {
        entry.addEventListener("click", (event) => {
            event.stopPropagation();
            entry.parentElement.classList.toggle("show-children");
        });
    });

    document.querySelectorAll(".entry ul.duplicates > li > a").forEach(dupeLink => {
        dupeLink.addEventListener("click", () => {
            const dupeId = (new URL(dupeLink.href)).hash.substring(1);
            let dupeAncestor = document.getElementById(dupeId).parentElement;
            while (dupeAncestor && dupeAncestor !== document.body) {
                if (dupeAncestor.classList.contains("entry")) {
                    dupeAncestor.classList.add("show-children");
                }
                dupeAncestor = dupeAncestor.parentElement;
            }
        });
    });

    const hideAllButton = document.createElement("button");
    hideAllButton.textContent = "Hide all";
    hideAllButton.addEventListener("click", () => {
        document.querySelectorAll(".entry.has-children").forEach(entry => {
            entry.classList.remove("show-children");
        });
    });
    document.body.prepend(hideAllButton);

    const showAllButton = document.createElement("button");
    showAllButton.textContent = "Show all";
    showAllButton.addEventListener("click", () => {
        document.querySelectorAll(".entry.has-children").forEach(entry => {
            entry.classList.add("show-children");
        });
    });
    document.body.prepend(showAllButton);
});
