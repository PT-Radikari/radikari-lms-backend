import { Context, TypedResponse } from "hono"
import * as TenantService from "$services/TenantService"
import * as UserService from "$services/UserService"
import {
	handleServiceErrorWithResponse,
	response_created,
	response_success,
} from "$utils/response.utils"
import { TenantCreateUpdateDTO } from "$entities/Tenant"
import * as EzFilter from "@nodewave/prisma-ezfilter"
import { TenantUserUpdateDTO } from "$entities/TenantUser"
import * as TenanUserService from "$services/TenanUserService"
import { UserJWTDAO } from "$entities/User"
import * as TenantRoleService from "$services/TenantRoleService"
import { Roles } from "../../../generated/prisma/client"
import { prisma } from "$pkg/prisma"
import { response_forbidden } from "$utils/response.utils"

export async function create(c: Context): Promise<TypedResponse> {
	const data: TenantCreateUpdateDTO = await c.req.json()
	const user: UserJWTDAO = c.get("jwtPayload")

	const serviceResponse = await TenantService.create(data, user.id)

	if (!serviceResponse.status) {
		return handleServiceErrorWithResponse(c, serviceResponse)
	}

	return response_created(
		c,
		serviceResponse.data,
		"Successfully created new Tenant!",
	)
}

export async function getAll(c: Context): Promise<TypedResponse> {
	const user: UserJWTDAO = c.get("jwtPayload")
	const filters: EzFilter.FilteringQuery = EzFilter.extractQueryFromParams(
		c.req.query(),
	)

	let serviceResponse
	if (user.role === Roles.ADMIN) {
		serviceResponse = await TenantService.getAll(filters)
	} else {
		serviceResponse = await TenantService.getAllByUserId(filters, user)
	}

	if (!serviceResponse.status) {
		return handleServiceErrorWithResponse(c, serviceResponse)
	}

	return response_success(
		c,
		serviceResponse.data,
		"Successfully fetched all Tenant!",
	)
}

export async function getById(c: Context): Promise<TypedResponse> {
	const id = c.req.param("id")

	const serviceResponse = await TenantService.getById(id)

	if (!serviceResponse.status) {
		return handleServiceErrorWithResponse(c, serviceResponse)
	}

	return response_success(
		c,
		serviceResponse.data,
		"Successfully fetched Tenant by id!",
	)
}

export async function update(c: Context): Promise<TypedResponse> {
	const data: TenantCreateUpdateDTO = await c.req.json()
	const id = c.req.param("id")
	const user: UserJWTDAO = c.get("jwtPayload")

	const serviceResponse = await TenantService.update(id, data, user.id)

	if (!serviceResponse.status) {
		return handleServiceErrorWithResponse(c, serviceResponse)
	}

	return response_success(
		c,
		serviceResponse.data,
		"Successfully updated Tenant!",
	)
}

export async function deleteById(c: Context): Promise<TypedResponse> {
	const id = c.req.param("id")
	const user: UserJWTDAO = c.get("jwtPayload")
	const serviceResponse = await TenantService.deleteById(id, user.id)

	if (!serviceResponse.status) {
		return handleServiceErrorWithResponse(c, serviceResponse)
	}

	return response_success(
		c,
		serviceResponse.data,
		"Successfully deleted Tenant!",
	)
}

export async function getInvitableUsers(c: Context): Promise<TypedResponse> {
	const tenantId = c.req.param("id")
	const filters: EzFilter.FilteringQuery = EzFilter.extractQueryFromParams(
		c.req.query(),
	)

	const serviceResponse = await UserService.getInvitableForTenant(
		tenantId,
		filters,
	)

	if (!serviceResponse.status) {
		return handleServiceErrorWithResponse(c, serviceResponse)
	}

	return response_success(
		c,
		serviceResponse.data,
		"Successfully fetched invitable users for tenant!",
	)
}

export async function getUserInTenant(c: Context): Promise<TypedResponse> {
	const id = c.req.param("id")

	const serviceResponse = await TenanUserService.getByTenantId(id)

	if (!serviceResponse.status) {
		return handleServiceErrorWithResponse(c, serviceResponse)
	}

	return response_success(
		c,
		serviceResponse.data,
		"Successfully fetched users in tenant!",
	)
}

export async function getAllTenantUsers(c: Context): Promise<TypedResponse> {
	const user: UserJWTDAO = c.get("jwtPayload")
	const filters: EzFilter.FilteringQuery = EzFilter.extractQueryFromParams(
		c.req.query(),
	)

	// [SECURITY] If not global ADMIN, must filter by own tenant
	if (user.role !== Roles.ADMIN) {
		let tenantIdFilter = ""
		let parsedFilters: any = []

		if (filters.filters) {
			try {
				parsedFilters =
					typeof filters.filters === "string"
						? JSON.parse(filters.filters as string)
						: filters.filters

				// Handle both Object and Array formats
				if (Array.isArray(parsedFilters)) {
					const found = parsedFilters.find((f: any) => f.key === "tenantId")
					tenantIdFilter = found?.value
				} else if (
					typeof parsedFilters === "object" &&
					parsedFilters !== null
				) {
					tenantIdFilter = (parsedFilters as any).tenantId
				}
			} catch (e) {
				/* ignore */
			}
		}

		if (!tenantIdFilter) {
			return response_forbidden(
				c,
				"Tenant ID filter is required for non-admins!",
			)
		}

		// Verify membership
		const membership = await prisma.tenantUser.findFirst({
			where: { userId: user.id, tenantId: tenantIdFilter },
		})

		if (!membership) {
			return response_forbidden(
				c,
				"You are not authorized to view users in this tenant!",
			)
		}
	}

	const serviceResponse = await TenanUserService.getAll(filters)

	if (!serviceResponse.status) {
		return handleServiceErrorWithResponse(c, serviceResponse)
	}

	return response_success(
		c,
		serviceResponse.data,
		"Successfully fetched all tenant users!",
	)
}

export async function assignUserToTenant(c: Context): Promise<TypedResponse> {
	const data: TenantUserUpdateDTO[] = await c.req.json()
	const id = c.req.param("id")

	const serviceResponse = await TenanUserService.assignUserTenantByTenantId(
		id,
		data,
	)

	if (!serviceResponse.status) {
		return handleServiceErrorWithResponse(c, serviceResponse)
	}

	return response_success(
		c,
		serviceResponse.data,
		"Successfully assigned user to tenant!",
	)
}

export async function getAllByUser(c: Context): Promise<TypedResponse> {
	const filters: EzFilter.FilteringQuery = EzFilter.extractQueryFromParams(
		c.req.query(),
	)
	const user: UserJWTDAO = c.get("jwtPayload")

	const serviceResponse = await TenantService.getAllByUserId(filters, user)

	if (!serviceResponse.status) {
		return handleServiceErrorWithResponse(c, serviceResponse)
	}

	return response_success(
		c,
		serviceResponse.data,
		"Successfully fetched all tenants by user id!",
	)
}

export async function getAllRoles(c: Context): Promise<TypedResponse> {
	const tenantId = c.req.query("tenantId")
	const serviceResponse = await TenantRoleService.getAll(tenantId)

	if (!serviceResponse.status) {
		return handleServiceErrorWithResponse(c, serviceResponse)
	}

	return response_success(
		c,
		serviceResponse.data,
		"Successfully fetched all roles!",
	)
}

export async function createTenantUser(c: Context): Promise<TypedResponse> {
	const data: TenantUserUpdateDTO = await c.req.json()
	const id = c.req.param("id")

	const serviceResponse = await TenanUserService.create(id, data)

	if (!serviceResponse.status) {
		return handleServiceErrorWithResponse(c, serviceResponse)
	}

	return response_success(
		c,
		serviceResponse.data,
		"Successfully created tenant user!",
	)
}

// [FIXED] Fungsi Add Member yang benar
export async function addMember(c: Context): Promise<TypedResponse> {
	const id = c.req.param("id")
	const body = await c.req.json()

	// Pastikan membaca tenantRoleId
	const { userId, tenantRoleId } = body

	// Validasi input
	if (!userId || !tenantRoleId) {
		return c.json(
			{
				status: false,
				message: "userId and tenantRoleId are required",
			},
			400,
		)
	}

	const serviceResponse = await TenantService.addMember(
		id,
		userId,
		tenantRoleId,
	)

	if (!serviceResponse.status) {
		return handleServiceErrorWithResponse(c, serviceResponse)
	}

	return response_success(c, serviceResponse.data, "Member successfully added!")
}

export async function upsertSetting(c: Context): Promise<TypedResponse> {
	const id = c.req.param("id")
	const body = await c.req.json()
	const user: UserJWTDAO = c.get("jwtPayload")

	const { key, value } = body

	if (!key || !value) {
		return c.json(
			{
				status: false,
				message: "key and value are required",
			},
			400,
		)
	}

	const serviceResponse = await TenantService.upsertSetting(
		id,
		key,
		value,
		user.id,
	)

	if (!serviceResponse.status) {
		return handleServiceErrorWithResponse(c, serviceResponse)
	}

	return response_success(
		c,
		serviceResponse.data,
		"Successfully updated tenant setting!",
	)
}

export async function getSettings(c: Context): Promise<TypedResponse> {
	const id = c.req.param("id")

	const serviceResponse = await TenantService.getSettings(id)

	if (!serviceResponse.status) {
		return handleServiceErrorWithResponse(c, serviceResponse)
	}

	return response_success(
		c,
		serviceResponse.data,
		"Successfully fetched tenant settings!",
	)
}

export async function getUserPoints(c: Context): Promise<TypedResponse> {
	const id = c.req.param("id")
	const userId = c.req.param("userId")

	const serviceResponse = await TenantService.getUserPoints(id, userId)

	if (!serviceResponse.status) {
		return handleServiceErrorWithResponse(c, serviceResponse)
	}

	return response_success(
		c,
		serviceResponse.data,
		"Successfully fetched user points!",
	)
}
