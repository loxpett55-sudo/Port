'use strict';

// ============================================================================
// Настройки анимации
// ============================================================================
const CONFIG = {
  SCENE_SIZE: 1024,      // родной размер кадра
  OVERSCAN: 1.04,        // запас масштаба, чтобы покачивание не оголяло края

  // Базовое покачивание всей сцены («дыхание»)
  SWAY_DEG: 0.45,        // амплитуда поворота, градусы
  SWAY_PERIOD: 5.5,      // период, сек
  BOB_PX: 4,             // вертикальное покачивание, px
  BOB_PERIOD: 7,
  BREATH_SCALE: 0.004,   // амплитуда «дыхания» масштабом
  BREATH_PERIOD: 6,

  // Независимое покачивание слоя волос/декора
  HAIR_DEG: 0.6,         // базовая амплитуда, градусы (2–4° в пике порыва)
  HAIR_PERIOD: 3.6,      // период, сек

  // Порывы ветра
  GUST_MIN_MS: 5000,     // случайный интервал между порывами 5–8 с
  GUST_MAX_MS: 8000,
  GUST_HOLD_MS: 1300,    // длительность порыва ~1–1.5 с
  GUST_AMP_BOOST: 2.2,   // множитель амплитуды в порыве
  GUST_SPEED_BOOST: 0.9, // прибавка к скорости в порыве
  GUST_EASE: 2.5,        // скорость нарастания/спада порыва (лерп/сек)

  // Моргание
  BLINK_MIN_MS: 4000,    // интервал между открытиями глаз 4–7 с
  BLINK_MAX_MS: 7000,
  PEEK_MS: 150,          // короткое «моргание» открытыми глазами
  AWAKE_CHANCE: 0.25,    // иногда глаза остаются открытыми подольше…
  AWAKE_MIN_MS: 1800,    // …и следят за курсором
  AWAKE_MAX_MS: 4000,

  // Слежение глаз за курсором
  EYE_MAX_PX: 4,         // максимальный сдвиг зрачков, px (в координатах сцены)
  EYE_LERP: 5,           // скорость сглаживания (лерп/сек)

  // Звёзды-искры
  SPARKLE_COUNT: 70,
};

const scene = document.getElementById('scene');
const hair = document.getElementById('hair');
const eyeLeft = document.getElementById('eye-left');
const eyeRight = document.getElementById('eye-right');
const sparklesCanvas = document.getElementById('sparkles');
const ctx = sparklesCanvas.getContext('2d');

const rand = (min, max) => min + Math.random() * (max - min);
const clamp = (v, min, max) => Math.min(max, Math.max(min, v));
// Экспоненциальный лерп, независимый от частоты кадров.
const damp = (current, target, rate, dt) =>
  current + (target - current) * (1 - Math.exp(-rate * dt));

// ============================================================================
// Масштабирование сцены под экран (cover)
// ============================================================================
let coverScale = 1;

function fitScene() {
  const { innerWidth: w, innerHeight: h } = window;
  coverScale =
    (Math.max(w, h) / CONFIG.SCENE_SIZE) * CONFIG.OVERSCAN;
  sparklesCanvas.width = w * devicePixelRatio;
  sparklesCanvas.height = h * devicePixelRatio;
  sparklesCanvas.style.width = `${w}px`;
  sparklesCanvas.style.height = `${h}px`;
  ctx.setTransform(devicePixelRatio, 0, 0, devicePixelRatio, 0, 0);
}

window.addEventListener('resize', () => {
  fitScene();
  initSparkles();
});

// ============================================================================
// Курсор: глобальные координаты из main-процесса (или mousemove как fallback)
// ============================================================================
// Нормализованная цель взгляда: -1..1 по обеим осям от центра экрана.
const gaze = { targetX: 0, targetY: 0, x: 0, y: 0 };

function setGazeTarget(nx, ny) {
  gaze.targetX = clamp(nx, -1, 1);
  gaze.targetY = clamp(ny, -1, 1);
}

if (window.desktopAPI) {
  window.desktopAPI.onCursor((c) => {
    const cx = c.screenX + c.screenW / 2;
    const cy = c.screenY + c.screenH / 2;
    setGazeTarget((c.x - cx) / (c.screenW / 2), (c.y - cy) / (c.screenH / 2));
  });
} else {
  // Запуск вне Electron (отладка в браузере)
  window.addEventListener('mousemove', (e) => {
    setGazeTarget(
      (e.clientX - innerWidth / 2) / (innerWidth / 2),
      (e.clientY - innerHeight / 2) / (innerHeight / 2)
    );
  });
}

// ============================================================================
// Моргание: закрыто → короткий взгляд (150 мс) или «пробуждение» на пару секунд
// ============================================================================
let eyesOpen = false;

function setEyes(open) {
  eyesOpen = open;
  eyeLeft.classList.toggle('open', open);
  eyeRight.classList.toggle('open', open);
}

function scheduleBlink() {
  setTimeout(() => {
    const awake = Math.random() < CONFIG.AWAKE_CHANCE;
    const holdMs = awake
      ? rand(CONFIG.AWAKE_MIN_MS, CONFIG.AWAKE_MAX_MS)
      : CONFIG.PEEK_MS;
    setEyes(true);
    setTimeout(() => {
      setEyes(false);
      scheduleBlink();
    }, holdMs);
  }, rand(CONFIG.BLINK_MIN_MS, CONFIG.BLINK_MAX_MS));
}

// ============================================================================
// Порывы ветра
// ============================================================================
let gustTarget = 0; // 0 — штиль, 1 — пик порыва
let gust = 0;

function scheduleGust() {
  setTimeout(() => {
    gustTarget = 1;
    setTimeout(() => {
      gustTarget = 0;
      scheduleGust();
    }, CONFIG.GUST_HOLD_MS);
  }, rand(CONFIG.GUST_MIN_MS, CONFIG.GUST_MAX_MS));
}

// ============================================================================
// Звёзды-искры поверх сцены
// ============================================================================
let sparkles = [];

function initSparkles() {
  sparkles = Array.from({ length: CONFIG.SPARKLE_COUNT }, () => ({
    x: Math.random() * innerWidth,
    y: Math.random() * innerHeight,
    r: rand(0.6, 2.2),
    vy: rand(-6, -1.5),          // медленно всплывают
    vx: rand(-2, 2),
    phase: rand(0, Math.PI * 2), // фаза мерцания
    speed: rand(0.6, 2.0),       // скорость мерцания
    hue: rand(200, 280),         // сине-фиолетовая палитра
  }));
}

function drawSparkles(t, dt) {
  ctx.clearRect(0, 0, innerWidth, innerHeight);
  ctx.globalCompositeOperation = 'lighter';
  const wind = 1 + gust * 6; // порыв разгоняет искры
  for (const s of sparkles) {
    s.x += s.vx * wind * dt;
    s.y += s.vy * dt;
    if (s.y < -5) { s.y = innerHeight + 5; s.x = Math.random() * innerWidth; }
    if (s.x < -5) s.x = innerWidth + 5;
    if (s.x > innerWidth + 5) s.x = -5;

    const tw = 0.35 + 0.65 * (0.5 + 0.5 * Math.sin(t * s.speed + s.phase));
    const g = ctx.createRadialGradient(s.x, s.y, 0, s.x, s.y, s.r * 3);
    g.addColorStop(0, `hsla(${s.hue}, 90%, 85%, ${0.9 * tw})`);
    g.addColorStop(1, `hsla(${s.hue}, 90%, 70%, 0)`);
    ctx.fillStyle = g;
    ctx.beginPath();
    ctx.arc(s.x, s.y, s.r * 3, 0, Math.PI * 2);
    ctx.fill();
  }
  ctx.globalCompositeOperation = 'source-over';
}

// ============================================================================
// Главный цикл анимации
// ============================================================================
let hairPhase = 0;
let lastT = performance.now();

function frame(now) {
  const dt = Math.min((now - lastT) / 1000, 0.1);
  lastT = now;
  const t = now / 1000;

  // Порыв ветра плавно нарастает и спадает.
  gust = damp(gust, gustTarget, CONFIG.GUST_EASE, dt);

  // Базовое покачивание всей сцены.
  const sway = CONFIG.SWAY_DEG * Math.sin((t * 2 * Math.PI) / CONFIG.SWAY_PERIOD);
  const bob = CONFIG.BOB_PX * Math.sin((t * 2 * Math.PI) / CONFIG.BOB_PERIOD + 1.3);
  const breath =
    1 + CONFIG.BREATH_SCALE * Math.sin((t * 2 * Math.PI) / CONFIG.BREATH_PERIOD);
  scene.style.transform =
    `translate(-50%, calc(-50% + ${bob.toFixed(2)}px)) ` +
    `scale(${(coverScale * breath).toFixed(4)}) ` +
    `rotate(${sway.toFixed(3)}deg)`;

  // Волосы/декор: своя фаза; порыв ветра увеличивает амплитуду и скорость.
  const hairSpeed =
    ((2 * Math.PI) / CONFIG.HAIR_PERIOD) * (1 + gust * CONFIG.GUST_SPEED_BOOST);
  hairPhase += hairSpeed * dt;
  const hairAmp = CONFIG.HAIR_DEG * (1 + gust * CONFIG.GUST_AMP_BOOST);
  const hairRot = hairAmp * Math.sin(hairPhase);
  hair.style.transform = `rotate(${hairRot.toFixed(3)}deg)`;

  // Глаза: при открытых — плавно тянутся к курсору, при закрытых — к центру.
  const ex = eyesOpen ? gaze.targetX * CONFIG.EYE_MAX_PX : 0;
  const ey = eyesOpen ? gaze.targetY * CONFIG.EYE_MAX_PX : 0;
  gaze.x = damp(gaze.x, ex, CONFIG.EYE_LERP, dt);
  gaze.y = damp(gaze.y, ey, CONFIG.EYE_LERP, dt);
  const eyeTransform = `translate(${gaze.x.toFixed(2)}px, ${gaze.y.toFixed(2)}px)`;
  eyeLeft.style.transform = eyeTransform;
  eyeRight.style.transform = eyeTransform;

  drawSparkles(t, dt);
  requestAnimationFrame(frame);
}

// ============================================================================
fitScene();
initSparkles();
scheduleBlink();
scheduleGust();
requestAnimationFrame(frame);
