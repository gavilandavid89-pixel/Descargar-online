(() => {
    'use strict';

    const PAGE_SIZE = 24;
    const state = {
        url: '',
        label: '',
        platform: '',
        sort: 'recent',
        supportedSorts: ['recent'],
        page: 0,
        cursors: [null],
        cache: new Map(),
        loading: false,
        requestId: 0,
        controller: null
    };

    const $ = (id) => document.getElementById(id);
    const view = () => $('creatorView');
    const grid = () => $('creatorGrid');
    const status = () => $('creatorStatus');
    const previous = () => $('creatorPrevious');
    const next = () => $('creatorNext');

    function pageKey(sort, cursor) {
        return sort + '|' + (cursor || 'FIRST');
    }

    function setBusy(busy) {
        state.loading = busy;
        previous().disabled = busy || state.page === 0;
        next().disabled = busy;
        $('creatorReload').disabled = busy;
        document.querySelectorAll('[data-creator-sort]').forEach(button => {
            const supported = state.supportedSorts.includes(button.dataset.creatorSort);
            button.disabled = busy || !supported;
            button.title = supported ? '' : 'Este orden no está disponible con la fuente pública de esta plataforma.';
            const active = button.dataset.creatorSort === state.sort;
            button.classList.toggle('border-b-2', active);
            button.classList.toggle('border-amber-300', active);
            button.classList.toggle('text-amber-200', active);
            button.classList.toggle('text-slate-400', !active);
            button.classList.toggle('font-semibold', active);
        });
    }

    function showHome() {
        view().classList.add('hidden');
        document.querySelector('body > main').classList.remove('hidden');
        const responsible = document.getElementById('responsibleSection');
        if (responsible) responsible.classList.remove('hidden');
        const footer = document.querySelector('body > footer');
        if (footer) footer.classList.remove('hidden');
        const adSlot = $('adSlot');
        if (adSlot) adSlot.classList.toggle('hidden', $('resultContainer').classList.contains('hidden'));
    }

    function showCreatorShell() {
        document.querySelector('body > main').classList.add('hidden');
        const responsible = document.getElementById('responsibleSection');
        if (responsible) responsible.classList.add('hidden');
        const footer = document.querySelector('body > footer');
        if (footer) footer.classList.add('hidden');
        const adSlot = $('adSlot');
        if (adSlot) adSlot.classList.add('hidden');
        view().classList.remove('hidden');
        window.scrollTo({ top: 0, behavior: 'instant' });
    }

    function makeText(tag, className, value) {
        const node = document.createElement(tag);
        node.className = className || '';
        node.textContent = value == null ? '' : String(value);
        return node;
    }

    function formatCompact(value) {
        const number = Number(value);
        return Number.isFinite(number) && number > 0
            ? new Intl.NumberFormat('es-AR', { notation: 'compact', maximumFractionDigits: 1 }).format(number)
            : '';
    }

    function formatDate(value) {
        if (!value) return '';
        const date = new Date(value);
        if (Number.isNaN(date.getTime())) return '';
        return new Intl.DateTimeFormat('es-AR', { year: 'numeric', month: 'short', day: 'numeric' }).format(date);
    }

    $('creatorDescriptionToggle').addEventListener('click', () => {
        const description = $('creatorDescription');
        const toggle = $('creatorDescriptionToggle');
        const expanded = toggle.getAttribute('aria-expanded') === 'true';
        description.classList.toggle('is-collapsed', expanded);
        toggle.setAttribute('aria-expanded', String(!expanded));
        toggle.textContent = expanded ? 'Ver descripción completa' : 'Mostrar menos';
    });

    function renderProfile(profile) {
        $('creatorName').textContent = profile.title || state.label || 'Perfil del creador';
        $('creatorPlatform').textContent = (profile.platform || state.platform || 'multiplataforma').toUpperCase();
        const avatar = $('creatorAvatar');
        avatar.onerror = () => {
            avatar.classList.add('hidden');
            $('creatorAvatarFallback').classList.remove('hidden');
        };
        if (profile.thumbnail) {
            avatar.src = profile.thumbnail;
            avatar.classList.remove('hidden');
            $('creatorAvatarFallback').classList.add('hidden');
        } else {
            avatar.removeAttribute('src');
            avatar.classList.add('hidden');
            $('creatorAvatarFallback').classList.remove('hidden');
        }
        const description = String(profile.description || '').trim();
        const descriptionElement = $('creatorDescription');
        const descriptionToggle = $('creatorDescriptionToggle');
        descriptionElement.textContent = description;
        descriptionElement.classList.toggle('hidden', !description);
        // Decide from actual rendered overflow, not a character-count guess.
        // Long unbroken URLs/short wide-screen bios should not create a needless toggle.
        descriptionElement.classList.remove('is-collapsed');
        descriptionToggle.hidden = true;
        descriptionToggle.style.display = 'none';
        descriptionToggle.textContent = 'Ver descripción completa';
        descriptionToggle.setAttribute('aria-expanded', 'false');
        if (description) {
            descriptionElement.classList.add('is-collapsed');
            requestAnimationFrame(() => {
                const hasMore = descriptionElement.scrollHeight > descriptionElement.clientHeight + 2;
                descriptionToggle.hidden = !hasMore;
                descriptionToggle.style.display = hasMore ? 'inline-block' : 'none';
                if (!hasMore) descriptionElement.classList.remove('is-collapsed');
            });
        }
        const details = [];
        if (profile.subscribers) details.push(formatCompact(profile.subscribers) + ' seguidores/suscriptores');
        $('creatorDetails').textContent = details.join(' · ');
    }

    function renderEntries(entries) {
        const container = grid();
        container.replaceChildren();

        if (!entries.length) {
            container.appendChild(makeText('p', 'col-span-full rounded-xl border border-slate-700 bg-slate-900/70 p-6 text-center text-slate-400', 'La fuente no devolvió más publicaciones para esta página.'));
            return;
        }

        entries.forEach((entry, index) => {
            const card = document.createElement('button');
            card.type = 'button';
            card.className = 'creator-card min-w-0 overflow-hidden border-0 bg-transparent p-0 text-left transition focus:outline-none focus:ring-2 focus:ring-amber-300';
            card.setAttribute('aria-label', 'Analizar publicación: ' + (entry.title || 'Sin título'));

            const media = document.createElement('div');
            media.className = 'creator-media relative bg-slate-800/50';
            if (entry.thumbnail) {
                const img = document.createElement('img');
                img.src = entry.thumbnail;
                img.alt = '';
                img.loading = 'lazy';
                img.decoding = 'async';
                img.referrerPolicy = 'no-referrer';
                img.className = 'aspect-video w-full object-cover';
                img.addEventListener('error', () => {
                    img.classList.add('hidden');
                    media.classList.add('creator-no-thumbnail');
                }, { once: true });
                media.appendChild(img);
            } else {
                media.classList.add('creator-no-thumbnail');
                media.appendChild(makeText('span', 'flex aspect-video items-center justify-center text-3xl text-amber-200/70', '▶'));
            }
            if (Number(entry.duration) > 0) {
                media.appendChild(makeText('span', 'absolute bottom-2 right-2 rounded bg-black/80 px-1.5 py-0.5 text-xs text-white', formatDurationSafe(entry.duration)));
            }
            card.appendChild(media);

            const body = document.createElement('div');
            body.className = 'space-y-1 pt-2';
            body.appendChild(makeText('h3', 'line-clamp-2 break-words text-[13px] font-medium leading-snug text-slate-100 sm:text-sm', entry.title || 'Publicación sin título'));
            const metadata = [];
            if (entry.views) metadata.push(formatCompact(entry.views) + ' visualizaciones');
            const date = formatDate(entry.publishedAt);
            if (date) metadata.push(date);
            body.appendChild(makeText('p', 'text-[11px] leading-snug text-slate-500', metadata.join(' · ') || 'Información adicional no disponible'));
            card.appendChild(body);

            card.addEventListener('click', () => {
                if (state.loading) return;
                showHome();
                const input = $('videoUrl');
                input.value = entry.url;
                const form = $('fetchForm');
                if (form) form.requestSubmit();
                window.scrollTo({ top: 0, behavior: 'smooth' });
            });
            container.appendChild(card);
        });
    }

    function formatDurationSafe(value) {
        const seconds = Math.max(0, Math.floor(Number(value) || 0));
        if (!seconds) return '';
        const hours = Math.floor(seconds / 3600);
        const minutes = Math.floor((seconds % 3600) / 60);
        const rest = seconds % 60;
        return hours
            ? hours + ':' + String(minutes).padStart(2, '0') + ':' + String(rest).padStart(2, '0')
            : minutes + ':' + String(rest).padStart(2, '0');
    }

    function renderPage(data, key) {
        state.supportedSorts = Array.isArray(data.supportedSorts) ? data.supportedSorts : [data.sort || 'recent'];
        renderProfile(data.creator || {});
        renderEntries(Array.isArray(data.entries) ? data.entries : []);
        $('creatorPageLabel').textContent = 'Página ' + (state.page + 1);
        $('creatorResultCount').textContent = (data.entries || []).length + ' en esta página';
        previous().disabled = state.loading || state.page === 0;
        next().disabled = state.loading || !data.nextCursor;
        next().classList.toggle('opacity-40', !data.nextCursor);
        next().setAttribute('aria-disabled', String(!data.nextCursor));
        $('creatorNotice').textContent = data.notice || '';
        $('creatorNotice').classList.toggle('hidden', !data.notice);
        state.cache.set(key, data);
    }

    async function loadPage(pageIndex, cursor, force) {
        if (state.loading) return;
        const key = pageKey(state.sort, cursor);
        const cached = !force ? state.cache.get(key) : null;
        state.page = pageIndex;
        showCreatorShell();

        if (cached) {
            renderPage(cached, key);
            previous().disabled = state.page === 0;
            next().disabled = !cached.nextCursor;
            $('creatorStatus').textContent = '';
            return;
        }

        const requestId = ++state.requestId;
        status().classList.remove('creator-error');
        setBusy(true);
        status().textContent = 'Consultando publicaciones reales…';
        $('creatorNotice').classList.add('hidden');
        grid().replaceChildren();
        grid().appendChild(makeText('p', 'col-span-full py-8 text-sm text-slate-400', 'Cargando publicaciones…'));

        try {
            const controller = new AbortController();
            state.controller = controller;
            const response = await fetch('/api/creator/feed', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                cache: 'no-store',
                signal: controller.signal,
                body: JSON.stringify({
                    url: state.url,
                    sort: state.sort,
                    cursor: cursor || '',
                    limit: PAGE_SIZE,
                    force: Boolean(force)
                })
            });
            const data = await response.json();
            if (requestId !== state.requestId) return;
            if (!response.ok) throw new Error(data.error || 'No se pudo consultar el perfil.');
            state.url = data.creator && data.creator.url ? data.creator.url : state.url;
            renderPage(data, key);
            status().textContent = '';
        } catch (error) {
            if (requestId !== state.requestId) return;
            grid().replaceChildren();
            status().textContent = error.message || 'No se pudo cargar el perfil.';
            status().classList.add('creator-error');
            next().disabled = true;
        } finally {
            if (requestId === state.requestId) {
                setBusy(false);
                previous().disabled = state.page === 0;
                const current = state.cache.get(pageKey(state.sort, state.cursors[state.page]));
                next().disabled = !current || !current.nextCursor;
            }
        }
    }

    function openCreator(url, label, platform) {
        if (!url) {
            status().textContent = 'No se pudo identificar el enlace del creador.';
            return;
        }
        if (state.controller) state.controller.abort();
        state.controller = null;
        state.loading = false;
        state.url = String(url).trim();
        state.label = label || '';
        state.platform = platform || '';
        state.sort = 'recent';
        state.supportedSorts = ['recent'];
        state.page = 0;
        state.cursors = [null];
        state.cache.clear();
        state.requestId++;
        $('creatorUrl').value = state.url;
        $('creatorNotice').classList.add('hidden');
        status().classList.remove('creator-error');

        // Actualizar la cabecera al instante para que nunca quede visible
        // información del perfil anterior mientras responde la nueva consulta.
        $('creatorName').textContent = state.label || 'Cargando perfil…';
        $('creatorPlatform').textContent = (state.platform || 'PERFIL').toUpperCase();
        $('creatorDetails').textContent = 'Consultando la fuente original…';
        $('creatorDescription').textContent = '';
        $('creatorDescription').classList.add('hidden');
        $('creatorAvatar').removeAttribute('src');
        $('creatorAvatar').classList.add('hidden');
        $('creatorAvatarFallback').classList.remove('hidden');
        $('creatorPageLabel').textContent = 'Página 1';
        $('creatorResultCount').textContent = 'Cargando';
        grid().replaceChildren();
        loadPage(0, null, false);
    }

    $('creatorForm').addEventListener('submit', event => {
        event.preventDefault();
        const url = $('creatorUrl').value.trim();
        if (!url) return;
        openCreator(url, '', '');
    });

    $('creatorHome').addEventListener('click', showHome);
    $('creatorReload').addEventListener('click', () => {
        const cursor = state.cursors[state.page] || null;
        state.cache.delete(pageKey(state.sort, cursor));
        loadPage(state.page, cursor, true);
    });

    previous().addEventListener('click', () => {
        if (state.loading || state.page <= 0) return;
        state.page -= 1;
        loadPage(state.page, state.cursors[state.page] || null, false);
    });

    next().addEventListener('click', () => {
        if (state.loading) return;
        const cursor = state.cursors[state.page] || null;
        const data = state.cache.get(pageKey(state.sort, cursor));
        if (!data || !data.nextCursor) return;
        state.cursors[state.page + 1] = data.nextCursor;
        state.page += 1;
        loadPage(state.page, state.cursors[state.page], false);
    });

    document.querySelectorAll('[data-creator-sort]').forEach(button => {
        button.addEventListener('click', () => {
            const sort = button.dataset.creatorSort;
            if (state.loading || !sort || sort === state.sort) return;
            state.sort = sort;
            state.page = 0;
            state.cursors = [null];
            state.cache.clear();
            loadPage(0, null, false);
        });
    });

    const originalShowResults = window.mostrarResultados;
    if (typeof originalShowResults === 'function') {
        window.mostrarResultados = function(data) {
            originalShowResults(data);
            const button = $('viewCreatorButton');
            if (!button) return;
            const canHaveCreator = data && data.mediaType !== 'image' && data.platform && data.platform !== 'generic';
            button.classList.toggle('hidden', !canHaveCreator);
            button.onclick = () => {
                const fallbackUrl = $('videoUrl').value.trim();
                openCreator(data.creatorUrl || data.channelUrl || fallbackUrl, data.uploader || '', data.platform || '');
            };
        };
    }

    window.MediaGrabCreator = { openCreator: openCreator, showHome: showHome };
})();