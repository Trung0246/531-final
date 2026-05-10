export async function graphqlRequest<T>(query: string, variables: Record<string, unknown> = {}): Promise<T> {
	const response = await fetch('/graphql', {
		method: 'POST',
		headers: {
			'Content-Type': 'application/json'
		},
		body: JSON.stringify({ query, variables })
	});

	const payload = await response.json();
	if (!response.ok || payload.errors?.length) {
		throw new Error(payload.errors?.[0]?.message ?? response.statusText);
	}

	return payload.data as T;
}
