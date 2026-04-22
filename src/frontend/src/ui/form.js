import { loginUrl } from '../auth.js';

const SAMPLE_PICKUP = '2026-04-21T09:00';

export const MODEL_FEATURES = [
  'pickup_hour',
  'pickup_dow',
  'pickup_month',
  'pickup_is_weekend',
  'pickup_borough_id',
  'pickup_zone_id',
  'pickup_service_zone_id',
  'dropoff_borough_id',
  'dropoff_zone_id',
  'dropoff_service_zone_id',
  'route_pair_id',
  'avg_duration_7d_zone_hour',
  'avg_fare_30d_zone',
  'trip_count_90d_zone_hour',
];

export const SAMPLE_VALUES = {
  pickup_datetime: SAMPLE_PICKUP,
  pickup_borough_id: 1,
  pickup_zone_id: 15,
  pickup_service_zone_id: 2,
  dropoff_borough_id: 1,
  dropoff_zone_id: 30,
  dropoff_service_zone_id: 2,
  route_pair_id: 123,
  avg_duration_7d_zone_hour: 500,
  avg_fare_30d_zone: 18,
  trip_count_90d_zone_hour: 60,
};

function escapeHtml(value) {
  return String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

function pad(number) {
  return String(number).padStart(2, '0');
}

function datetimeFromInput(value) {
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? null : date;
}

function computeTimeFeatures(date) {
  if (!date) {
    return {
      pickup_hour: '',
      pickup_dow: '',
      pickup_month: '',
      pickup_is_weekend: '',
    };
  }

  const day = date.getDay();
  return {
    pickup_hour: date.getHours(),
    pickup_dow: day,
    pickup_month: date.getMonth() + 1,
    pickup_is_weekend: day === 0 || day === 6 ? 1 : 0,
  };
}

function numericValue(form, name) {
  const field = form.elements.namedItem(name);
  const raw = field && 'value' in field ? String(field.value).trim() : '';
  if (raw === '') {
    throw new Error(`${name} is required`);
  }
  const value = Number(raw);
  if (!Number.isFinite(value)) {
    throw new Error(`${name} must be numeric`);
  }
  return value;
}

function renderFeatureRow({ name, label, help, type = 'number', step = '1', min, max, value = '', required = true }) {
  return `
    <label class="field">
      <span class="field__label">${escapeHtml(label)}${required ? ' <em>*</em>' : ''}</span>
      <input
        class="input"
        name="${escapeHtml(name)}"
        type="${escapeHtml(type)}"
        ${step ? `step="${escapeHtml(step)}"` : ''}
        ${min !== undefined ? `min="${escapeHtml(min)}"` : ''}
        ${max !== undefined ? `max="${escapeHtml(max)}"` : ''}
        value="${escapeHtml(value)}"
        ${required ? 'required' : ''}
      />
      ${help ? `<span class="field__help">${escapeHtml(help)}</span>` : ''}
    </label>
  `;
}

export function renderPredictionPage({ authState }) {
  const signedIn = Boolean(authState?.authenticated);
  const userLabel = authState?.user?.name || authState?.user?.email || 'Signed in';
  const defaultDatetime = SAMPLE_PICKUP;

  return `
    <section class="hero-card card">
      <div class="hero-card__copy">
        <p class="eyebrow">Prediction</p>
        <h1>Structured trip input, one click prediction.</h1>
        <p class="lead">
          The model expects 13 numeric inputs. This page derives the time fields from a single pickup datetime,
          keeps the rest in named fields, and submits the payload in the exact shape the backend expects.
        </p>
      </div>
      <div class="hero-card__meta">
        <div class="stat">
          <span class="stat__value">13</span>
          <span class="stat__label">required model inputs</span>
        </div>
        <div class="stat">
          <span class="stat__value">1</span>
          <span class="stat__label">prediction per submit</span>
        </div>
      </div>
    </section>

    <section class="card auth-banner ${signedIn ? 'auth-banner--ok' : 'auth-banner--warn'}" data-auth-banner>
      <div>
        <p class="eyebrow">Session</p>
        <h2>${signedIn ? 'Authenticated' : 'Authentication required'}</h2>
        <p class="muted">${signedIn ? `Signed in as ${escapeHtml(userLabel)}.` : 'Use the top-right button to sign in before predicting.'}</p>
      </div>
      <div class="auth-banner__actions">
        ${signedIn
          ? '<span class="pill pill--ok">Ready to predict</span>'
          : `<a class="button button--login" href="${loginUrl('/predict.html')}">Login</a>`}
      </div>
    </section>

    <section class="card">
      <form id="predict-form" class="predict-form" novalidate>
        <div class="form-toolbar">
          <div>
            <h2>Plan a ride</h2>
            <p class="muted">Use the sample trip to get a valid prediction immediately, then edit the fields you care about.</p>
          </div>
          <div class="button-row">
            <button type="button" class="button button--ghost" data-action="sample">Load sample trip</button>
            <button type="button" class="button button--ghost" data-action="reset">Reset</button>
            <button type="submit" class="button button--primary" data-action="submit" ${signedIn ? '' : 'disabled'}>Predict</button>
          </div>
        </div>

        <fieldset class="predict-form__fieldset" ${signedIn ? '' : 'disabled'}>
          <div class="section-grid">
            <section class="section-card">
              <div class="section-head">
                <h3>Trip time</h3>
                <p class="muted">Pickup datetime drives the hour, day-of-week, month, and weekend flags.</p>
              </div>

              <label class="field field--full">
                <span class="field__label">Pickup datetime <em>*</em></span>
                <input class="input" name="pickup_datetime" type="datetime-local" value="${escapeHtml(defaultDatetime)}" required />
                <span class="field__help">Derived fields update automatically.</span>
              </label>

              <div class="derived-grid" aria-live="polite">
                <div class="derived-chip"><span>pickup_hour</span><strong data-derived="pickup_hour">9</strong></div>
                <div class="derived-chip"><span>pickup_dow</span><strong data-derived="pickup_dow">2</strong></div>
                <div class="derived-chip"><span>pickup_month</span><strong data-derived="pickup_month">4</strong></div>
                <div class="derived-chip"><span>pickup_is_weekend</span><strong data-derived="pickup_is_weekend">0</strong></div>
              </div>

              <input type="hidden" name="pickup_hour" value="9" />
              <input type="hidden" name="pickup_dow" value="2" />
              <input type="hidden" name="pickup_month" value="4" />
              <input type="hidden" name="pickup_is_weekend" value="0" />
            </section>

            <section class="section-card">
              <div class="section-head">
                <h3>Pickup and dropoff IDs</h3>
                <p class="muted">These are model inputs, not free-text labels. Use the sample to start from a known-good combination.</p>
              </div>
              <div class="feature-grid">
                ${renderFeatureRow({ name: 'pickup_borough_id', label: 'Pickup borough ID', help: 'Numeric borough identifier.' , value: '1'})}
                ${renderFeatureRow({ name: 'pickup_zone_id', label: 'Pickup zone ID', help: 'Numeric zone identifier.', value: '15'})}
                ${renderFeatureRow({ name: 'pickup_service_zone_id', label: 'Pickup service zone ID', help: 'Numeric service zone identifier.', value: '2'})}
                ${renderFeatureRow({ name: 'dropoff_borough_id', label: 'Dropoff borough ID', help: 'Numeric borough identifier.', value: '1'})}
                ${renderFeatureRow({ name: 'dropoff_zone_id', label: 'Dropoff zone ID', help: 'Numeric zone identifier.', value: '30'})}
                ${renderFeatureRow({ name: 'dropoff_service_zone_id', label: 'Dropoff service zone ID', help: 'Numeric service zone identifier.', value: '2'})}
              </div>
            </section>

            <section class="section-card section-card--wide">
              <div class="section-head">
                <h3>Model-specific features</h3>
                <p class="muted">These engineered values are required by the model. Keep the sample defaults unless you know the correct values for your trip.</p>
              </div>
              <div class="feature-grid feature-grid--two-up">
                ${renderFeatureRow({ name: 'route_pair_id', label: 'Route pair ID', help: 'Engineered route identifier.', value: '123'})}
                ${renderFeatureRow({ name: 'avg_duration_7d_zone_hour', label: 'Average duration 7d zone hour', help: 'Historical average duration.', step: '0.1', value: '500'})}
                ${renderFeatureRow({ name: 'avg_fare_30d_zone', label: 'Average fare 30d zone', help: 'Historical average fare.', step: '0.1', value: '18'})}
                ${renderFeatureRow({ name: 'trip_count_90d_zone_hour', label: 'Trip count 90d zone hour', help: 'Historical trip count.', value: '60'})}
              </div>
            </section>
          </div>
        </fieldset>
      </form>
    </section>

    <section class="card">
      <div class="section-head">
        <h2>Result</h2>
        <p class="muted">Prediction output appears here after a successful request.</p>
      </div>
      <pre id="predict-result" class="result">No prediction yet.</pre>
    </section>
  `;
}

function setDerivedFields(form) {
  const raw = String(form.elements.namedItem('pickup_datetime')?.value || '').trim();
  const date = datetimeFromInput(raw);
  const derived = computeTimeFeatures(date);

  for (const [name, value] of Object.entries(derived)) {
    const hidden = form.elements.namedItem(name);
    if (hidden && 'value' in hidden) hidden.value = String(value);
    const display = form.querySelector(`[data-derived="${name}"]`);
    if (display) display.textContent = String(value);
  }
}

export function fillSampleTrip(form) {
  const values = {
    pickup_datetime: SAMPLE_VALUES.pickup_datetime,
    pickup_borough_id: SAMPLE_VALUES.pickup_borough_id,
    pickup_zone_id: SAMPLE_VALUES.pickup_zone_id,
    pickup_service_zone_id: SAMPLE_VALUES.pickup_service_zone_id,
    dropoff_borough_id: SAMPLE_VALUES.dropoff_borough_id,
    dropoff_zone_id: SAMPLE_VALUES.dropoff_zone_id,
    dropoff_service_zone_id: SAMPLE_VALUES.dropoff_service_zone_id,
    route_pair_id: SAMPLE_VALUES.route_pair_id,
    avg_duration_7d_zone_hour: SAMPLE_VALUES.avg_duration_7d_zone_hour,
    avg_fare_30d_zone: SAMPLE_VALUES.avg_fare_30d_zone,
    trip_count_90d_zone_hour: SAMPLE_VALUES.trip_count_90d_zone_hour,
  };

  for (const [name, value] of Object.entries(values)) {
    const field = form.elements.namedItem(name);
    if (field && 'value' in field) field.value = String(value);
  }

  setDerivedFields(form);
  persistDraft(form);
}

export function resetTrip(form) {
  form.reset();
  const defaults = {
    pickup_datetime: SAMPLE_VALUES.pickup_datetime,
    pickup_borough_id: 1,
    pickup_zone_id: 15,
    pickup_service_zone_id: 2,
    dropoff_borough_id: 1,
    dropoff_zone_id: 30,
    dropoff_service_zone_id: 2,
    route_pair_id: 123,
    avg_duration_7d_zone_hour: 500,
    avg_fare_30d_zone: 18,
    trip_count_90d_zone_hour: 60,
  };
  for (const [name, value] of Object.entries(defaults)) {
    const field = form.elements.namedItem(name);
    if (field && 'value' in field) field.value = String(value);
  }
  setDerivedFields(form);
  persistDraft(form);
}

function persistDraft(form) {
  const draft = {};
  for (const name of MODEL_FEATURES) {
    const field = form.elements.namedItem(name);
    if (field && 'value' in field) draft[name] = String(field.value);
  }
  const pickup = form.elements.namedItem('pickup_datetime');
  if (pickup && 'value' in pickup) draft.pickup_datetime = String(pickup.value);
  localStorage.setItem('tabular-predict-draft', JSON.stringify(draft));
}

export function restoreDraft(form) {
  const raw = localStorage.getItem('tabular-predict-draft');
  if (!raw) {
    setDerivedFields(form);
    return;
  }

  try {
    const draft = JSON.parse(raw);
    if (!draft || typeof draft !== 'object') {
      setDerivedFields(form);
      return;
    }

    for (const [name, value] of Object.entries(draft)) {
      const field = form.elements.namedItem(name);
      if (field && 'value' in field) field.value = String(value);
    }
    setDerivedFields(form);
  } catch {
    setDerivedFields(form);
  }
}

export function readPredictionInstance(form) {
  setDerivedFields(form);

  return {
    pickup_hour: numericValue(form, 'pickup_hour'),
    pickup_dow: numericValue(form, 'pickup_dow'),
    pickup_month: numericValue(form, 'pickup_month'),
    pickup_is_weekend: numericValue(form, 'pickup_is_weekend'),
    pickup_borough_id: numericValue(form, 'pickup_borough_id'),
    pickup_zone_id: numericValue(form, 'pickup_zone_id'),
    pickup_service_zone_id: numericValue(form, 'pickup_service_zone_id'),
    dropoff_borough_id: numericValue(form, 'dropoff_borough_id'),
    dropoff_zone_id: numericValue(form, 'dropoff_zone_id'),
    dropoff_service_zone_id: numericValue(form, 'dropoff_service_zone_id'),
    route_pair_id: numericValue(form, 'route_pair_id'),
    avg_duration_7d_zone_hour: numericValue(form, 'avg_duration_7d_zone_hour'),
    avg_fare_30d_zone: numericValue(form, 'avg_fare_30d_zone'),
    trip_count_90d_zone_hour: numericValue(form, 'trip_count_90d_zone_hour'),
  };
}

export function buildPredictionRequest(form) {
  return {
    instances: [readPredictionInstance(form)],
  };
}

export function wirePredictionForm(root, { onSubmit, authenticated }) {
  const form = root.querySelector('#predict-form');
  if (!form) return () => {};

  const result = root.querySelector('#predict-result');
  const submitButton = form.querySelector('[data-action="submit"]');
  const sampleButton = form.querySelector('[data-action="sample"]');
  const resetButton = form.querySelector('[data-action="reset"]');

  const updateDraft = () => {
    persistDraft(form);
  };

  const onChange = () => {
    setDerivedFields(form);
    updateDraft();
  };

  form.addEventListener('input', onChange);
  form.addEventListener('change', onChange);

  sampleButton?.addEventListener('click', () => {
    fillSampleTrip(form);
    if (result) result.textContent = 'Sample trip loaded. Submit to generate a prediction.';
  });

  resetButton?.addEventListener('click', () => {
    resetTrip(form);
    if (result) result.textContent = 'Form reset.';
  });

  form.addEventListener('submit', async (event) => {
    event.preventDefault();
    event.stopPropagation();

    if (!authenticated()) {
      if (typeof onSubmit === 'function') {
        await onSubmit({ kind: 'auth_required' });
      }
      return;
    }

    if (result) result.textContent = 'Loading prediction...';
    if (submitButton) submitButton.disabled = true;

    try {
      const payload = buildPredictionRequest(form);
      if (typeof onSubmit === 'function') {
        await onSubmit({ kind: 'predict', payload });
      }
    } catch (error) {
      if (result) {
        result.textContent = error instanceof Error ? error.message : 'Unable to submit prediction.';
      }
    } finally {
      if (submitButton) submitButton.disabled = !authenticated();
    }
  });

  setDerivedFields(form);
  restoreDraft(form);
  if (!localStorage.getItem('tabular-predict-draft')) {
    fillSampleTrip(form);
  }

  if (submitButton) submitButton.disabled = !authenticated();

  return () => {
    form.removeEventListener('input', onChange);
    form.removeEventListener('change', onChange);
  };
}

export function renderPredictionResult(root, payload) {
  const result = root.querySelector('#predict-result');
  if (!result) return;
  result.textContent = JSON.stringify(payload, null, 2);
}

export function setPredictBanner(root, { authenticated, userLabel }) {
  const banner = root.querySelector('[data-auth-banner]');
  if (!banner) return;
  const heading = banner.querySelector('h2');
  const paragraph = banner.querySelector('.muted');
  const pill = banner.querySelector('.pill');

  if (authenticated) {
    banner.classList.remove('auth-banner--warn');
    banner.classList.add('auth-banner--ok');
    if (heading) heading.textContent = 'Authenticated';
    if (paragraph) paragraph.textContent = `Signed in as ${userLabel}.`;
    if (pill) {
      pill.textContent = 'Ready to predict';
      pill.className = 'pill pill--ok';
    }
  } else {
    banner.classList.remove('auth-banner--ok');
    banner.classList.add('auth-banner--warn');
    if (heading) heading.textContent = 'Authentication required';
    if (paragraph) paragraph.textContent = 'Use the top-right button to sign in before predicting.';
    if (pill) {
      pill.textContent = 'Login required';
      pill.className = 'pill pill--warn';
    }
  }
}
