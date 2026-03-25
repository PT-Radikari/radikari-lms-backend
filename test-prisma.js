const { PrismaClient } = require('@prisma/client');
const prisma = new PrismaClient();
async function main() {
  const log = await prisma.userActivityLog.findFirst();
  console.log(log);
}
main()
  .catch(e => console.error(e))
  .finally(async () => {
    await prisma.$disconnect();
  });
