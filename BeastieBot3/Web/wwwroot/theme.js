// The Theme control in the header (System, Light or Dark).
//
// index.html loads this file in <head> without defer, so data-theme is set on <html> before the
// page is drawn and a dark page never shows in light colours first. style.css reads data-theme:
// "light" and "dark" override the system setting, "system" follows it. The choice is kept in this
// browser's localStorage for this address and port. When storage is blocked the choice still
// applies, but only until the page is reloaded.
(function () {
  'use strict';

  const KEY = 'theme';
  const root = document.documentElement;

  function read() {
    try {
      const value = window.localStorage.getItem(KEY);
      return value === 'light' || value === 'dark' ? value : 'system';
    } catch (e) {
      return 'system';
    }
  }

  function save(value) {
    try {
      if (value === 'system') window.localStorage.removeItem(KEY);
      else window.localStorage.setItem(KEY, value);
    } catch (e) {
      // Storage is blocked: the theme applies until the page is reloaded.
    }
  }

  function apply(value) {
    root.setAttribute('data-theme', value === 'light' || value === 'dark' ? value : 'system');
  }

  apply(read());

  function setUp() {
    const select = document.getElementById('theme-select');
    if (!select) return;
    const refresh = () => {
      apply(read());
      select.value = root.getAttribute('data-theme');
    };
    select.value = root.getAttribute('data-theme');
    select.addEventListener('change', () => {
      apply(select.value);
      save(select.value);
    });
    // A choice made in another tab of the web UI.
    window.addEventListener('storage', (event) => {
      if (event.key === KEY || event.key === null) refresh();
    });
    window.addEventListener('pageshow', (event) => {
      if (event.persisted) refresh();
    });
    root.setAttribute('data-theme-ready', '');
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', setUp);
  else setUp();
})();
