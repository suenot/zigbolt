// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

// https://astro.build/config
export default defineConfig({
	site: 'https://zigbolt.dev',
	integrations: [
		starlight({
			title: 'ZigBolt',
			description: 'Ultra-Low Latency Messaging System for HFT',
			social: [
				{ icon: 'github', label: 'GitHub', href: 'https://github.com/suenot/zigbolt' },
			],
			logo: {
				light: './src/assets/zigbolt-logo-light.svg',
				dark: './src/assets/zigbolt-logo-dark.svg',
				replacesTitle: false,
			},
			customCss: ['./src/styles/custom.css'],
			sidebar: [
				{
					label: 'Getting Started',
					items: [
						{ label: 'Introduction', slug: 'getting-started/introduction' },
						{ label: 'Quick Start', slug: 'getting-started/quick-start' },
						{ label: 'Installation', slug: 'getting-started/installation' },
					],
				},
				{
					label: 'Architecture',
					items: [
						{ label: 'Overview', slug: 'architecture/overview' },
					],
				},
				{
					label: 'API Reference',
					items: [
						{ label: 'Full Reference', slug: 'reference/api-reference' },
					],
				},
				{
					label: 'Examples',
					items: [
						{ label: 'Usage Examples', slug: 'examples/usage' },
					],
				},
				{
					label: 'Performance',
					items: [
						{ label: 'Benchmarks', slug: 'performance/benchmarks' },
					],
				},
				{
					label: 'Changelog',
					autogenerate: { directory: 'changelog' },
				},
			],
		}),
	],
});
