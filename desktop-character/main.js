'use strict';

const { app, BrowserWindow, Tray, Menu, screen, nativeImage } = require('electron');
const path = require('path');
const fs = require('fs');

// Частота глобального опроса позиции курсора (≈30 раз/сек).
const CURSOR_POLL_MS = 33;

let win = null;
let tray = null;
let cursorTimer = null;

const settingsPath = () => path.join(app.getPath('userData'), 'settings.json');

function readSettings() {
  try {
    return JSON.parse(fs.readFileSync(settingsPath(), 'utf8'));
  } catch {
    return {};
  }
}

function writeSettings(patch) {
  const next = { ...readSettings(), ...patch };
  try {
    fs.writeFileSync(settingsPath(), JSON.stringify(next, null, 2));
  } catch (err) {
    console.error('Не удалось сохранить настройки:', err);
  }
  return next;
}

// --- Автозапуск -------------------------------------------------------------
// setLoginItemSettings на Windows пишет запись в
// HKCU\Software\Microsoft\Windows\CurrentVersion\Run.
// Регистрируем только упакованное приложение — в dev-режиме (npm start)
// process.execPath указывает на electron.exe, автозапуск не имеет смысла.

function isAutostartEnabled() {
  if (!app.isPackaged) return false;
  return app.getLoginItemSettings().openAtLogin;
}

function setAutostart(enable) {
  if (!app.isPackaged) return;
  app.setLoginItemSettings({
    openAtLogin: enable,
    path: process.execPath,
  });
}

function ensureAutostartOnFirstRun() {
  const settings = readSettings();
  if (app.isPackaged && !settings.firstRunDone) {
    setAutostart(true);
    writeSettings({ firstRunDone: true });
  }
}

// --- Окно-обои ---------------------------------------------------------------

function createWindow() {
  win = new BrowserWindow({
    fullscreen: true,
    frame: false,
    resizable: false,
    movable: false,
    transparent: false,
    skipTaskbar: true,
    // Окно не перехватывает фокус: клики по нему не отбирают клавиатуру
    // у активных приложений — поведение, близкое к обоям.
    focusable: false,
    // На Linux тип 'desktop' помещает окно на слой рабочего стола.
    // На Windows истинная интеграция под иконки требует трюка с WorkerW
    // (SetParent) — см. README, раздел «Интеграция как обои Windows».
    ...(process.platform === 'linux' ? { type: 'desktop' } : {}),
    webPreferences: {
      preload: path.join(__dirname, 'preload.js'),
      contextIsolation: true,
      nodeIntegration: false,
      // Анимация не должна замирать, когда окно не в фокусе.
      backgroundThrottling: false,
    },
  });

  win.setMenuBarVisibility(false);
  win.loadFile(path.join(__dirname, 'renderer', 'index.html'));

  win.on('closed', () => {
    win = null;
    stopCursorPolling();
  });

  startCursorPolling();
}

// --- Глобальное слежение за курсором ----------------------------------------
// screen.getCursorScreenPoint() работает даже когда окно не в фокусе,
// координаты пересылаются в renderer для эффекта «глаза следят за мышью».

function startCursorPolling() {
  stopCursorPolling();
  cursorTimer = setInterval(() => {
    if (!win || win.isDestroyed()) return;
    const point = screen.getCursorScreenPoint();
    const display = screen.getDisplayNearestPoint(point);
    win.webContents.send('cursor-pos', {
      x: point.x,
      y: point.y,
      screenX: display.bounds.x,
      screenY: display.bounds.y,
      screenW: display.bounds.width,
      screenH: display.bounds.height,
    });
  }, CURSOR_POLL_MS);
}

function stopCursorPolling() {
  if (cursorTimer) {
    clearInterval(cursorTimer);
    cursorTimer = null;
  }
}

// --- Трей --------------------------------------------------------------------

function createTray() {
  const icon = nativeImage.createFromPath(path.join(__dirname, 'assets', 'tray.png'));
  tray = new Tray(icon);
  tray.setToolTip('StarFluff — живые обои');
  refreshTrayMenu();
}

function refreshTrayMenu() {
  const menu = Menu.buildFromTemplate([
    {
      label: 'Автозапуск при входе в систему',
      type: 'checkbox',
      checked: isAutostartEnabled(),
      enabled: app.isPackaged,
      click: (item) => {
        setAutostart(item.checked);
        refreshTrayMenu();
      },
    },
    { type: 'separator' },
    {
      label: 'Выход',
      click: () => app.quit(),
    },
  ]);
  tray.setContextMenu(menu);
}

// --- Жизненный цикл ----------------------------------------------------------

const gotLock = app.requestSingleInstanceLock();
if (!gotLock) {
  app.quit();
} else {
  app.whenReady().then(() => {
    ensureAutostartOnFirstRun();
    createWindow();
    createTray();

    app.on('activate', () => {
      if (BrowserWindow.getAllWindows().length === 0) createWindow();
    });
  });

  app.on('window-all-closed', () => {
    app.quit();
  });
}
