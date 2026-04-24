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
					label: 'Start',
					items: [
						{ label: 'Overview', slug: '' },
						{ label: 'Getting Started', slug: 'getting-started' },
					],
				},
				{
					label: 'Concepts',
					items: [
						{ label: 'Roles', slug: 'concepts/roles' },
						{ label: 'Topology', slug: 'concepts/topology' },
						{ label: 'Traffic Patterns', slug: 'concepts/traffic-patterns' },
					],
				},
				{
					label: 'Architecture',
					items: [
						{ label: 'Overview', slug: 'architecture/overview' },
						{ label: 'Bridge Protocol', slug: 'architecture/bridge-protocol' },
						{ label: 'NATS Transport', slug: 'architecture/nats-transport' },
						{ label: 'TCP Sessions', slug: 'architecture/tcp-sessions' },
						{ label: 'Cancellation', slug: 'architecture/cancellation' },
					],
				},
				{
					label: 'Configuration',
					items: [
						{ label: 'Environment', slug: 'configuration/environment' },
						{ label: 'Proxy Auth', slug: 'configuration/proxy-auth' },
						{ label: 'SOCKS5', slug: 'configuration/socks5' },
					],
				},
				{
					label: 'Deployment',
					items: [
						{ label: 'External NATS', slug: 'deployment/external-nats' },
						{ label: 'Embedded NATS', slug: 'deployment/embedded-nats' },
						{ label: 'Self-NATS Leafnodes', slug: 'deployment/self-nats-leafnodes' },
						{ label: 'Docker', slug: 'deployment/docker' },
					],
				},
				{
					label: 'Operations',
					items: [
						{ label: 'Observability', slug: 'operations/observability' },
						{ label: 'Healthcheck', slug: 'operations/healthcheck' },
						{ label: 'Troubleshooting', slug: 'operations/troubleshooting' },
					],
				},
				{
					label: 'Development',
					items: [
						{ label: 'Local Dev', slug: 'development/local-dev' },
						{ label: 'Testing', slug: 'development/testing' },
						{ label: 'Code Map', slug: 'development/code-map' },
					],
				},
			],
		}),
	],
});
