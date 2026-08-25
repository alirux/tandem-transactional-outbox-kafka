/*
 * <tandem-message-flow> — the animated "how a message moves through Tandem" panel.
 *
 * Self-contained custom element: markup, styles and behaviour all live here, inside a shadow
 * root, so dropping <tandem-message-flow></tandem-message-flow> into any page is enough — no
 * other include, no build step (matches this site's "no framework, no toolchain" rule).
 *
 * Reads the page's own palette through inherited custom properties (--bg, --green, --mono, ...,
 * already set on :root by assets/style.css) with hardcoded fallbacks, so it also renders
 * reasonably on a page that never defines them.
 *
 * Usage:
 *   <script src="assets/message-flow.js" defer></script>
 *   ...
 *   <tandem-message-flow></tandem-message-flow>
 */
(function () {
  var TEMPLATE = document.createElement('template');
  TEMPLATE.innerHTML =
    '<style>' +
    ':host{' +
    '  display:block;' +
    '  --_bg:var(--bg,#080a0f);--_panel:var(--panel,#0d1119);--_panel-raised:var(--panel-raised,#161c28);' +
    '  --_border:var(--border,#212b3b);--_text:var(--text,#ffffff);--_text-soft:var(--text-soft,#c6d0e0);' +
    '  --_muted:var(--muted,#7e8ca8);--_green:var(--green,#12b412);--_yellow:var(--yellow,#d8d000);' +
    '  --_red:var(--red,#e03a1a);' +
    '  --_sans:var(--sans,"Helvetica Neue",Helvetica,Arial,system-ui,sans-serif);' +
    '  --_mono:var(--mono,Menlo,"DejaVu Sans Mono","SF Mono",Consolas,monospace);' +
    '  font-family:var(--_sans);color:var(--_text-soft);' +
    '}' +
    '*{box-sizing:border-box;}' +
    'code{font-family:var(--_mono);font-size:0.9em;color:var(--_text);}' +

    '.diagram-frame{background:var(--_panel);border:1px solid var(--_border);border-radius:14px;padding:26px 22px 20px;}' +
    '.diagram-scroll{overflow-x:auto;}' +
    '.diagram{min-width:760px;}' +

    '.diagram-note{margin-top:18px;padding-top:16px;border-top:1px solid var(--_border);color:var(--_muted);font-size:14.5px;}' +
    '.diagram-note code{font-size:0.9em;}' +
    '.diagram-controls{display:flex;justify-content:flex-end;gap:8px;margin-bottom:16px;}' +
    '.btn-toggle{display:inline-flex;align-items:center;gap:7px;background:var(--_panel-raised);border:1px solid var(--_border);border-radius:8px;color:var(--_text-soft);font-family:var(--_mono);font-size:12.5px;padding:7px 14px;cursor:pointer;}' +
    '.btn-toggle:hover:not(:disabled){border-color:var(--_green);color:var(--_text);}' +
    '.btn-toggle:focus-visible{outline:2px solid var(--_green);outline-offset:2px;}' +
    '.btn-toggle .icon{font-size:13px;line-height:1;}' +
    '.btn-toggle:disabled{opacity:.35;cursor:not-allowed;}' +

    '.stage-row{display:grid;grid-template-columns:132px 1fr;gap:0;margin-bottom:6px;}' +
    '.stage-track{position:relative;height:40px;}' +
    '.stage{position:absolute;top:0;transform:translateX(-50%);text-align:center;width:130px;}' +
    '.stage .n{display:inline-flex;align-items:center;justify-content:center;width:20px;height:20px;border-radius:50%;border:1px solid var(--_border);color:var(--_muted);font-size:11px;font-family:var(--_mono);margin-bottom:4px;}' +
    '.stage .t{display:block;font-size:12.5px;color:var(--_text-soft);font-weight:600;}' +
    '.stage.s1{left:6%;} .stage.s2{left:34%;} .stage.s3{left:62%;} .stage.s4{left:90%;}' +

    '.lane{display:grid;grid-template-columns:132px 1fr;border-top:1px dashed var(--_border);align-items:center;}' +
    '.lane:first-of-type{border-top:1px solid var(--_border);}' +
    '.lane-label{padding:14px 12px 14px 0;}' +
    '.lane-label .tag{display:block;font-size:9px;text-transform:uppercase;letter-spacing:.08em;color:var(--_muted);margin-bottom:3px;}' +
    '.lane-label .agg{display:block;font-family:var(--_mono);font-size:12.5px;color:var(--_text);}' +
    '.lane-label .bucket{display:block;font-size:11px;color:var(--_muted);margin-top:2px;}' +
    '.lane-track{position:relative;height:76px;}' +
    '.lane-track .rail{position:absolute;left:6%;right:10%;top:50%;height:1px;background:var(--_border);transform:translateY(-1px);}' +
    '.lane-track .stop{position:absolute;top:50%;width:6px;height:6px;border-radius:50%;background:var(--_border);transform:translate(-50%,-50%);}' +
    '.lane-track .stop.s1{left:6%;} .lane-track .stop.s2{left:34%;} .lane-track .stop.s3{left:62%;} .lane-track .stop.s4{left:90%;}' +

    '.card-pos{position:absolute;top:50%;left:6%;transform:translate(-50%,-50%);width:118px;height:34px;border-radius:8px;border:1px solid var(--_muted);background:var(--_panel-raised);animation-name:tmf-travel;animation-timing-function:ease-in-out;animation-iteration-count:infinite;}' +
    '.card-pos .lbl{position:absolute;inset:0;display:flex;align-items:center;justify-content:center;font-size:11px;font-family:var(--_mono);color:var(--_text-soft);white-space:nowrap;opacity:0;}' +

    /* border-color is repeated at each dwell stop (50%, 72%) as well as where it first changes
       (18%, 54%): without it the browser interpolates continuously across the whole dwell toward
       whatever color the *next* stop declares, so a paused frame mid-dwell shows a color already
       drifting toward the following stage. */
    '@keyframes tmf-travel{' +
    '  0%   { left:6%;  opacity:0; border-color:var(--_border); background:var(--_panel-raised); }' +
    '  3%   { opacity:1; }' +
    '  16%  { left:6%;  border-color:var(--_border); }' +
    '  18%  { left:34%; border-color:var(--_muted); }' +
    '  50%  { left:34%; border-color:var(--_muted); }' +
    '  54%  { left:62%; border-color:var(--_yellow); }' +
    '  72%  { left:62%; border-color:var(--_yellow); }' +
    '  74%  { left:90%; border-color:var(--_green); }' +
    '  96%  { opacity:1; }' +
    '  100% { left:90%; opacity:0; }' +
    '}' +
    '@keyframes tmf-lbl-commit  { 0%,14%{opacity:1;} 20%,100%{opacity:0;} }' +
    '@keyframes tmf-lbl-pending { 0%,14%{opacity:0;} 20%,50%{opacity:1;} 56%,100%{opacity:0;} }' +
    '@keyframes tmf-lbl-inflight{ 0%,52%{opacity:0;} 58%,70%{opacity:1;} 76%,100%{opacity:0;} }' +
    '@keyframes tmf-lbl-done    { 0%,72%{opacity:0;} 78%,94%{opacity:1;} 100%{opacity:0;} }' +

    '.card-pos .lbl.commit  { animation-name:tmf-lbl-commit;  animation-timing-function:linear; animation-iteration-count:infinite; color:var(--_text-soft); }' +
    '.card-pos .lbl.pending { animation-name:tmf-lbl-pending; animation-timing-function:linear; animation-iteration-count:infinite; color:var(--_muted); }' +
    '.card-pos .lbl.inflight{ animation-name:tmf-lbl-inflight;animation-timing-function:linear; animation-iteration-count:infinite; color:var(--_yellow); }' +
    '.card-pos .lbl.done    { animation-name:tmf-lbl-done;    animation-timing-function:linear; animation-iteration-count:infinite; color:var(--_green); }' +

    /* every animated element on a lane shares one clock, so its labels stay in phase with its
       motion. --dur lives on .lane and --delay on .card-pos; custom properties inherit down to
       .lbl either way. */
    '.card-pos, .card-pos .lbl{ animation-duration:var(--dur); animation-delay:var(--delay); }' +

    /* the poison lane: stalls at the relay stage instead of reaching Kafka */
    '.lane.poison .card-pos{ animation-name:tmf-travel-poison; }' +
    '@keyframes tmf-travel-poison{' +
    '  0%   { left:6%;  opacity:0; border-color:var(--_border); background:var(--_panel-raised); }' +
    '  3%   { opacity:1; }' +
    '  16%  { left:6%;  border-color:var(--_border); }' +
    '  18%  { left:34%; border-color:var(--_muted); }' +
    '  50%  { left:34%; border-color:var(--_muted); }' +
    '  54%  { left:62%; border-color:var(--_red); }' +
    '  60%  { box-shadow:0 0 0 3px rgba(224,58,26,.25); }' +
    '  66%  { box-shadow:none; }' +
    '  72%  { box-shadow:0 0 0 3px rgba(224,58,26,.25); }' +
    '  78%  { box-shadow:none; }' +
    '  84%  { box-shadow:0 0 0 3px rgba(224,58,26,.25); }' +
    '  90%  { box-shadow:none; left:62%; border-color:var(--_red); }' +
    /* fades out over the last 4%, mirroring tmf-travel's 96→100 fade: looping restarts the
       keyframe function from 0% with no interpolation across the seam, so without this the card
       sat fully opaque through 100% and popped to invisible the instant the next cycle began. */
    '  96%  { opacity:1; left:62%; border-color:var(--_red); }' +
    '  100% { left:62%; opacity:0; border-color:var(--_red); }' +
    '}' +
    '.lane.poison .lbl.blocked{ animation-name:tmf-lbl-blocked; animation-timing-function:linear; animation-iteration-count:infinite; color:var(--_red); }' +
    '@keyframes tmf-lbl-blocked{ 0%,52%{opacity:0;} 58%,100%{opacity:1;} }' +

    '.queued{position:absolute;left:34%;top:50%;transform:translate(-130%,-50%);display:flex;gap:4px;align-items:center;}' +
    '.queued .stack{position:relative;width:34px;height:20px;}' +
    '.queued .stack span{position:absolute;width:20px;height:14px;border-radius:4px;border:1px solid var(--_border);background:var(--_panel);}' +
    '.queued .stack span:nth-child(1){left:0;top:6px;}' +
    '.queued .stack span:nth-child(2){left:5px;top:3px;}' +
    '.queued .stack span:nth-child(3){left:10px;top:0;}' +
    '.queued small{color:var(--_muted);font-size:10.5px;white-space:nowrap;}' +

    '.legend{display:flex;flex-wrap:wrap;gap:18px;margin-top:18px;padding-top:16px;border-top:1px solid var(--_border);}' +
    '.legend span{display:inline-flex;align-items:center;gap:6px;font-size:12.5px;color:var(--_muted);}' +
    '.legend .dot{width:8px;height:8px;border-radius:50%;}' +

    '.narration{display:grid;gap:10px;margin-top:18px;padding-top:16px;border-top:1px solid var(--_border);}' +
    '.narration .line{display:grid;grid-template-columns:132px 1fr;gap:0;align-items:baseline;}' +
    '.narration .who{font-family:var(--_mono);font-size:12px;color:var(--_muted);padding-right:12px;}' +
    '.narration .say{font-size:14.5px;color:var(--_text-soft);min-height:1.4em;}' +
    '.narration .line.poison .who{color:var(--_red);}' +

    '@media (max-width:900px){' +
    '  .diagram-note{font-size:13px;}' +
    '  .narration .say{font-size:13px;}' +
    '  .narration .who{font-size:11px;padding-right:0;}' +
    '  .narration .line{grid-template-columns:1fr;gap:2px;}' +
    '  .diagram{min-width:500px;}' +
    '  .stage-row, .lane{grid-template-columns:80px 1fr;}' +
    '  .stage{width:100px;}' +
    '  .stage .n{width:16px;height:16px;font-size:9.5px;margin-bottom:2px;}' +
    '  .stage .t{font-size:10px;}' +
    '  .lane-label{padding:8px 6px 8px 0;}' +
    '  .lane-label .tag{font-size:7.5px;}' +
    '  .lane-label .agg{font-size:10.5px;}' +
    '  .lane-label .bucket{font-size:9.5px;}' +
    '  .lane-track{height:56px;}' +
    '  .card-pos{width:84px;height:27px;}' +
    '  .card-pos .lbl{font-size:9px;}' +
    '  .queued .stack{width:24px;height:14px;}' +
    '  .queued .stack span{width:14px;height:10px;}' +
    '  .queued small{font-size:8.5px;}' +
    '}' +
    /* A phone turned sideways is wide but very short. No "and (orientation:landscape)": if the
       host renders this inside a container narrower than the real screen, that container can end
       up short without being wider than tall, so it would never match "landscape" even while
       genuinely needing this tightened rhythm — height alone is the real signal. */
    '@media (max-height:500px){' +
    '  .diagram-note{font-size:11.5px;margin-top:10px;padding-top:10px;}' +
    '  .legend{margin-top:10px;padding-top:10px;gap:10px;}' +
    '  .legend span{font-size:11px;}' +
    '  .narration{margin-top:10px;padding-top:10px;gap:6px;}' +
    '  .narration .say{font-size:11.5px;}' +
    '  .narration .who{font-size:10px;}' +
    '}' +
    /* Reduced motion slows the loop rather than freezing it outright — the animation is the
       content here, so a full stop would lose the diagram, not just calm it down. */
    '@media (prefers-reduced-motion: reduce){' +
    '  .card-pos, .card-pos .lbl{ animation-duration:28s !important; }' +
    '}' +
    '</style>' +

    '<div class="diagram-frame">' +
    '  <div class="diagram-controls">' +
    '    <button type="button" class="btn-toggle" data-tmf="step-back" disabled title="Step back one stage (only while paused)">' +
    '      <span class="icon" aria-hidden="true">⏮</span><span class="label">Back</span>' +
    '    </button>' +
    '    <button type="button" class="btn-toggle" data-tmf="toggle-anim" aria-pressed="false">' +
    '      <span class="icon" aria-hidden="true">⏸</span><span class="label">Pause</span>' +
    '    </button>' +
    '    <button type="button" class="btn-toggle" data-tmf="step-fwd" disabled title="Step forward one stage (only while paused)">' +
    '      <span class="icon" aria-hidden="true">⏭</span><span class="label">Forward</span>' +
    '    </button>' +
    '  </div>' +
    '  <div class="diagram-scroll">' +
    '    <div class="diagram">' +
    '      <div class="stage-row">' +
    '        <div class="spacer"></div>' +
    '        <div class="stage-track">' +
    '          <div class="stage s1"><span class="n">1</span><span class="t">Commit</span></div>' +
    '          <div class="stage s2"><span class="n">2</span><span class="t">tandem_outbox</span></div>' +
    '          <div class="stage s3"><span class="n">3</span><span class="t">Relay claims</span></div>' +
    '          <div class="stage s4"><span class="n">4</span><span class="t">Kafka publish</span></div>' +
    '        </div>' +
    '      </div>' +

    '      <div class="lane" style="--dur:7s">' +
    '        <div class="lane-label"><span class="tag">aggregate_id</span><span class="agg">order-4471</span><span class="bucket">bucket 42</span></div>' +
    '        <div class="lane-track">' +
    '          <div class="rail"></div>' +
    '          <div class="stop s1"></div><div class="stop s2"></div><div class="stop s3"></div><div class="stop s4"></div>' +
    '          <div class="card-pos" data-tmf="card-main" style="--delay:0s">' +
    '            <span class="lbl commit">commit</span><span class="lbl pending">PENDING</span><span class="lbl inflight">IN_FLIGHT</span><span class="lbl done">DONE</span>' +
    '          </div>' +
    '        </div>' +
    '      </div>' +

    '      <div class="lane" style="--dur:8.4s">' +
    '        <div class="lane-label"><span class="tag">aggregate_id</span><span class="agg">order-2205</span><span class="bucket">bucket 187</span></div>' +
    '        <div class="lane-track">' +
    '          <div class="rail"></div>' +
    '          <div class="stop s1"></div><div class="stop s2"></div><div class="stop s3"></div><div class="stop s4"></div>' +
    '          <div class="card-pos" data-tmf="card-secondary" style="--delay:-4.2s">' +
    '            <span class="lbl commit">commit</span><span class="lbl pending">PENDING</span><span class="lbl inflight">IN_FLIGHT</span><span class="lbl done">DONE</span>' +
    '          </div>' +
    '        </div>' +
    '      </div>' +

    '      <div class="lane poison" style="--dur:9s">' +
    '        <div class="lane-label"><span class="tag">aggregate_id</span><span class="agg">order-9930</span><span class="bucket">bucket 187</span></div>' +
    '        <div class="lane-track">' +
    '          <div class="rail"></div>' +
    '          <div class="stop s1"></div><div class="stop s2"></div><div class="stop s3"></div><div class="stop s4"></div>' +
    '          <div class="queued"><div class="stack"><span></span><span></span><span></span></div><small>+3 queued, same aggregate</small></div>' +
    '          <div class="card-pos" data-tmf="card-poison" style="--delay:0s">' +
    '            <span class="lbl commit">commit</span><span class="lbl pending">PENDING</span><span class="lbl blocked">FAILED · blocked</span>' +
    '          </div>' +
    '        </div>' +
    '      </div>' +
    '    </div>' +
    '  </div>' +

    '  <p class="diagram-note">Each card is one outbox event, not an order (<code>order-4471</code>, <code>order-2205</code> and <code>order-9930</code> are just its <code>aggregate_id</code>, the Kafka partition key).</p>' +

    '  <div class="legend">' +
    '    <span><span class="dot" style="background:var(--_text-soft)"></span>written this transaction</span>' +
    '    <span><span class="dot" style="background:var(--_muted)"></span>PENDING (waiting in its bucket)</span>' +
    '    <span><span class="dot" style="background:var(--_yellow)"></span>IN_FLIGHT (claimed, publishing)</span>' +
    '    <span><span class="dot" style="background:var(--_green)"></span>DONE (acked by Kafka)</span>' +
    '    <span><span class="dot" style="background:var(--_red)"></span>FAILED (retries exhausted, aggregate stalled)</span>' +
    '  </div>' +

    '  <div class="narration">' +
    '    <div class="line"><span class="who">order-4471</span><span class="say" data-tmf="narrate-main">Commit (the domain write and the outbox row land in the same transaction).</span></div>' +
    '    <div class="line"><span class="who">order-2205</span><span class="say" data-tmf="narrate-secondary">Commit (written like any other row, same bucket as order-9930).</span></div>' +
    '    <div class="line poison"><span class="who">order-9930</span><span class="say" data-tmf="narrate-poison">Commit (written like any other row, same bucket as order-2205).</span></div>' +
    '  </div>' +
    '</div>';

  function wire(root) {
    var cardMain = root.querySelector('[data-tmf="card-main"]');
    var cardSecondary = root.querySelector('[data-tmf="card-secondary"]');
    var cardPoison = root.querySelector('[data-tmf="card-poison"]');

    function mainAnimOf(el) { return el.getAnimations()[0]; }
    var animMain = mainAnimOf(cardMain);
    var animSecondary = mainAnimOf(cardSecondary);
    var animPoison = mainAnimOf(cardPoison);

    function durationOf(anim) { return anim.effect.getTiming().duration; }

    function phaseOf(anim) {
      var d = durationOf(anim);
      var delay = anim.effect.getTiming().delay;
      var t = (anim.currentTime || 0) - delay;
      var m = t % d;
      if (m < 0) m += d;
      return Math.round((m / d) * 100000) / 1000;
    }

    var mainSteps = [
      { max: 16, text: "Commit (the domain write and the outbox row land in the same transaction). Nothing has read it yet." },
      { max: 52, text: "PENDING in tandem_outbox, bucket 42 (waiting its turn behind anything else already queued in that bucket)." },
      { max: 74, text: "Relay claims it (a worker takes the row with FOR UPDATE SKIP LOCKED and marks it IN_FLIGHT under a lease)." },
      { max: 100, text: "Published (a CloudEvent lands on Kafka keyed by aggregate_id; the broker's ack marks the row DONE)." }
    ];
    var secondarySteps = [
      { max: 16, text: "Commit (written like any other row, same bucket as order-9930)." },
      { max: 52, text: "PENDING in bucket 187 (a different aggregate_id than order-9930, so a separate claim target even though they share a bucket)." },
      { max: 74, text: "Relay claims it (SKIP LOCKED steps straight past order-9930's stuck row to take this one instead)." },
      { max: 100, text: "Published, DONE, keyed by aggregate_id order-2205 (order-9930, right below, is still stuck)." }
    ];
    var poisonSteps = [
      { max: 16, text: "Commit (written like any other row, same bucket as order-2205)." },
      { max: 52, text: "PENDING in bucket 187 (sitting behind order-2205, waiting for the relay)." },
      { max: 100, text: "FAILED · blocked (retries exhausted). Only order-9930 stalls; order-2205, right behind it in the same bucket, keeps flowing." }
    ];

    function pick(steps, phasePct) {
      for (var i = 0; i < steps.length; i++) { if (phasePct < steps[i].max) return steps[i].text; }
      return steps[steps.length - 1].text;
    }

    var elMain = root.querySelector('[data-tmf="narrate-main"]');
    var elSecondary = root.querySelector('[data-tmf="narrate-secondary"]');
    var elPoison = root.querySelector('[data-tmf="narrate-poison"]');

    function setText(el, text) { if (el.textContent !== text) el.textContent = text; }

    function tick() {
      setText(elMain, pick(mainSteps, phaseOf(animMain)));
      setText(elSecondary, pick(secondarySteps, phaseOf(animSecondary)));
      setText(elPoison, pick(poisonSteps, phaseOf(animPoison)));
    }

    tick();
    var tickId = setInterval(tick, 200);

    function allAnims() {
      var list = [];
      root.querySelectorAll('.card-pos, .card-pos .lbl').forEach(function (el) {
        el.getAnimations().forEach(function (a) { list.push(a); });
      });
      return list;
    }

    var toggleBtn = root.querySelector('[data-tmf="toggle-anim"]');
    var toggleIcon = toggleBtn.querySelector('.icon');
    var toggleLabel = toggleBtn.querySelector('.label');
    var stepBackBtn = root.querySelector('[data-tmf="step-back"]');
    var stepFwdBtn = root.querySelector('[data-tmf="step-fwd"]');
    var isPaused = false;

    function setPaused(p) {
      isPaused = p;
      allAnims().forEach(function (a) { p ? a.pause() : a.play(); });
      toggleBtn.setAttribute('aria-pressed', String(p));
      toggleIcon.textContent = p ? '▶' : '⏸';
      toggleLabel.textContent = p ? 'Resume' : 'Pause';
      stepBackBtn.disabled = !p;
      stepFwdBtn.disabled = !p;
      tick();
    }
    toggleBtn.addEventListener('click', function () { setPaused(!isPaused); });

    var STAGE_STARTS = [0.06, 0.35, 0.64, 0.86];

    function step(direction) {
      if (!isPaused) return;
      var d = durationOf(animMain);
      var eps = 1;
      var t = (animMain.currentTime || 0) % d;
      if (t < 0) t += d;
      var delta;
      if (direction > 0) {
        delta = (d + STAGE_STARTS[0] * d) - t;
        for (var i = 0; i < STAGE_STARTS.length; i++) {
          var s = STAGE_STARTS[i] * d;
          if (s > t + eps) { delta = s - t; break; }
        }
      } else {
        delta = STAGE_STARTS[STAGE_STARTS.length - 1] * d - t - d;
        for (var j = STAGE_STARTS.length - 1; j >= 0; j--) {
          var s2 = STAGE_STARTS[j] * d;
          if (s2 < t - eps) { delta = s2 - t; break; }
        }
      }
      allAnims().forEach(function (a) {
        var dd = a.effect.getTiming().duration;
        var ct = (a.currentTime || 0) + delta;
        ct = ((ct % dd) + dd) % dd;
        a.currentTime = Math.round(ct);
      });
      tick();
    }
    stepBackBtn.addEventListener('click', function () { step(-1); });
    stepFwdBtn.addEventListener('click', function () { step(1); });

    return function cleanup() { clearInterval(tickId); };
  }

  class TandemMessageFlow extends HTMLElement {
    connectedCallback() {
      if (this._wired) return;
      this._wired = true;
      var root = this.attachShadow({ mode: 'open' });
      root.appendChild(TEMPLATE.content.cloneNode(true));
      this._cleanup = wire(root);
    }
    disconnectedCallback() {
      if (this._cleanup) this._cleanup();
    }
  }

  if (!customElements.get('tandem-message-flow')) {
    customElements.define('tandem-message-flow', TandemMessageFlow);
  }
})();
