import { Hono } from "hono"
import * as TenantController from "$controllers/rest/TenantController"
import * as AuthMiddleware from "$middlewares/authMiddleware"
import * as Validations from "$validations/TenantValidation"
import { Roles } from "../../generated/prisma/client"
import * as UserKnowledgeReadLogController from "$controllers/rest/UserKnowledgeReadLogController"

const TenantRoutes = new Hono()

// Admin list tenants
TenantRoutes.get("/", AuthMiddleware.checkJwt, TenantController.getAll)

/**
 * ✅ Knowledge read logs (spesifik dulu, sebelum "/:id")
 */

// Admin checker: list logs per tenant
TenantRoutes.get(
	"/:id/knowledge-reads",
	AuthMiddleware.checkJwt,
	UserKnowledgeReadLogController.getAll,
)

TenantRoutes.post(
	"/:id/knowledge-reads/view",
	AuthMiddleware.checkJwt,
	// AuthMiddleware.checkRoleInTenant,
	UserKnowledgeReadLogController.markViewed,
)

// User get status in tenant
TenantRoutes.get(
	"/:id/knowledge/:knowledgeId/read-status",
	AuthMiddleware.checkJwt,
	AuthMiddleware.checkRoleInTenant,
	UserKnowledgeReadLogController.getStatus,
)

TenantRoutes.get(
	"/users/all",
	AuthMiddleware.checkJwt,
	TenantController.getAllTenantUsers,
)

TenantRoutes.get(
	"/users",
	AuthMiddleware.checkJwt,
	TenantController.getAllByUser,
)

TenantRoutes.get(
	"/roles",
	AuthMiddleware.checkJwt,
	TenantController.getAllRoles,
)

// Admin create tenant
TenantRoutes.post(
	"/",
	AuthMiddleware.checkJwt,
	AuthMiddleware.checkRole([Roles.ADMIN]),
	Validations.validateTenantSchema,
	TenantController.create,
)

// Admin update tenant
TenantRoutes.put(
	"/:id",
	AuthMiddleware.checkJwt,
	AuthMiddleware.checkRole([Roles.ADMIN]),
	Validations.validateTenantSchema,
	TenantController.update,
)

TenantRoutes.get(
	"/:id/invitable-users",
	AuthMiddleware.checkJwt,
	AuthMiddleware.checkAccessTenantRole("USER_MANAGEMENT", "CREATE"),
	TenantController.getInvitableUsers,
)

TenantRoutes.get(
	"/:id/users",
	AuthMiddleware.checkJwt,
	TenantController.getUserInTenant,
)

TenantRoutes.post(
	"/:id/users",
	AuthMiddleware.checkJwt,
	AuthMiddleware.checkAccessTenantRole("USER_MANAGEMENT", "CREATE"),
	TenantController.addMember,
)

TenantRoutes.put(
	"/:id/users",
	AuthMiddleware.checkJwt,
	AuthMiddleware.checkAccessTenantRole("USER_MANAGEMENT", "UPDATE"),
	Validations.validateTenantUserUpdateSchema,
	TenantController.assignUserToTenant,
)

TenantRoutes.delete(
	"/:id",
	AuthMiddleware.checkJwt,
	AuthMiddleware.checkRole([Roles.ADMIN]),
	TenantController.deleteById,
)

TenantRoutes.put(
	"/:id/settings",
	AuthMiddleware.checkJwt,
	TenantController.upsertSetting,
)

TenantRoutes.get(
	"/:id/settings",
	AuthMiddleware.checkJwt,
	TenantController.getSettings,
)

TenantRoutes.get(
	"/:id/users/:userId/points",
	AuthMiddleware.checkJwt,
	TenantController.getUserPoints,
)

// ✅ Taruh yang general paling bawah
TenantRoutes.get(
	"/:id",
	AuthMiddleware.checkJwt,
	AuthMiddleware.checkRole([Roles.ADMIN]),
	TenantController.getById,
)

export default TenantRoutes
