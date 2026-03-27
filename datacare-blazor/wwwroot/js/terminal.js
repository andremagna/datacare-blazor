window.datacareTerminal = {
    scrollToBottom: function (id) {
        var el = document.getElementById(id);
        if (el) el.scrollTop = el.scrollHeight;
    }
};
