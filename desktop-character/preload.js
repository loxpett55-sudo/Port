'use strict';

const { contextBridge, ipcRenderer } = require('electron');

contextBridge.exposeInMainWorld('desktopAPI', {
  // Подписка на глобальную позицию курсора, присылаемую main-процессом.
  onCursor(callback) {
    ipcRenderer.on('cursor-pos', (_event, data) => callback(data));
  },
});
