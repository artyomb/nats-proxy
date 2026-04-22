// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

const repoUrl = 'https://github.com/artyomb/nats-proxy';

export default defineConfig({
	site: 'https://artyomb.github.io',
	base: '/nats-proxy',
	integrations: [
		starlight({
			title: 'NATS Proxy',
			description: 'Documentation for the NATS Proxy Ruby gem.',
			social: [{ icon: 'github', label: 'GitHub', href: repoUrl }],
			editLink: {
				baseUrl: `${repoUrl}/edit/main/docs-site/`,
			},
			sidebar: [
				{
					label: 'Docs',
					items: [
						{ label: 'Hello', slug: 'hello' },
					],
				},
			],
		}),
	],
});
